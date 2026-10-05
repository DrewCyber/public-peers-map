package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"ymonitor/internal/ygg"
)

// PollerConfig tunes the poll loop.
type PollerConfig struct {
	PollEvery      time.Duration // between cycles (default 5m)
	ListTTL        time.Duration // publicnodes refresh interval (default 24h)
	Probe          ProbeOptions
	VanishAfter    time.Duration // present-but-silent longer than this -> vanish; 0 = 3*PollEvery
	HeartbeatEvery time.Duration // rewrite rows of stable peers at least this often; 0 = 15m
	PublicNodesURL string        // empty = default
	CustomPeers    []string      // URIs to monitor even when absent from the public list
}

// Poller runs collect cycles against one embedded node.
type Poller struct {
	node *ygg.Node
	store Store
	wal   WALStore
	cfg   PollerConfig
	log   *slog.Logger

	pubList   map[string]*ListPeer // publicnodes.json peers, by key
	pubUp     int
	list      map[string]*ListPeer // pubList + custom peers, by key
	listUp    int
	listAt    time.Time
	tr        *Tracker
	addedPeer map[string]bool // own peering URIs already added at runtime

	nodeKey string

	started time.Time
	mu      sync.Mutex
	stats   Stats
}

// Stats is served on the local health endpoint.
type Stats struct {
	UptimeS      int64  `json:"uptime_s"`
	Cycles       int64  `json:"cycles"`
	LastCycleTS  int64  `json:"last_cycle_ts"`
	LastOKTS     int64  `json:"last_ok_ts"`
	LastError    string `json:"last_error,omitempty"`
	PeersListed  int    `json:"peers_listed"`
	PeersUpList  int    `json:"peers_up_listed"`
	LastAnswered int    `json:"last_answered"`
	WALPending   int    `json:"wal_pending"`
	NodeKey      string `json:"node_key"`
}

func NewPoller(node *ygg.Node, store Store, wal WALStore, cfg PollerConfig, log *slog.Logger) *Poller {
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 5 * time.Minute
	}
	if cfg.ListTTL <= 0 {
		cfg.ListTTL = 24 * time.Hour
	}
	if cfg.VanishAfter <= 0 {
		cfg.VanishAfter = 3 * cfg.PollEvery
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = 15 * time.Minute
	}
	return &Poller{
		node:       node,
		store:      store,
		wal:        wal,
		cfg:        cfg,
		log:        log,
		addedPeer:  map[string]bool{},
		started:    time.Now(),
		nodeKey:    node.PublicKey(),
	}
}

// CycleOnce runs one full collect cycle. It returns an error when the cycle
// could not be completed; skipped-but-healthy cycles (e.g. too few answers)
// return nil so that -once exits 0 only after a genuinely good poll.
func (p *Poller) CycleOnce(ctx context.Context) error {
	now := time.Now()

	if err := p.refreshList(ctx); err != nil {
		return err
	}

	// The tracker must exist before the custom-peer steps: the key wait
	// treats a DB row as a resolved key, and a currently-down custom peer
	// recovers its key from the stored rows at merge time.
	if p.tr == nil {
		rows, err := p.store.FetchPeers()
		if err != nil {
			return p.setErr("bootstrap fetch /v1/peers: " + err.Error())
		}
		p.tr = NewTracker()
		p.tr.Bootstrap(rows)
		p.log.Info("tracker bootstrapped from API", "rows", len(rows))
	}
	if err := p.ensureCustomPeerings(ctx); err != nil {
		return err
	}
	if err := p.ensurePeerings(ctx); err != nil {
		return err
	}
	p.mergeCustomPeers()

	keys := make([]string, 0, len(p.list))
	for k := range p.list {
		keys = append(keys, k)
	}
	sample := SampleCoords(ctx, p.node, keys, p.cfg.Probe, p.log)

	// Safety valve: with no (or almost no) answers the sample says nothing
	// about the network — never touch the tracker or history in that case.
	p.mu.Lock()
	p.stats.Cycles++
	p.stats.LastCycleTS = now.Unix()
	p.stats.LastAnswered = len(sample)
	p.stats.PeersListed = len(p.list)
	p.stats.PeersUpList = p.listUp
	p.mu.Unlock()
	minAnswers := p.listUp * 3 / 10
	if minAnswers < 1 {
		minAnswers = 1
	}
	if len(sample) < minAnswers {
		p.setErrf("only %d/%d answers (min %d) — cycle skipped, state untouched", len(sample), len(p.list), minAnswers)
		return nil
	}

	ts := time.Now().Unix()
	diff := Diff(p.list, sample, p.tr, DiffOptions{
		Now:            ts,
		VanishWindow:   p.cfg.VanishAfter,
		HeartbeatEvery: p.cfg.HeartbeatEvery,
	})
	batch := Batch{PollTS: ts, Peers: diff.Peers, Events: diff.Events}

	if err := p.wal.Append(batch); err != nil {
		return p.setErr("wal append: " + err.Error())
	}
	if err := p.flushWAL(); err != nil {
		return err // batch stays in the WAL for the next attempt
	}

	p.mu.Lock()
	p.stats.LastOKTS = ts
	p.mu.Unlock()
	p.log.Info("cycle done",
		"answered", len(sample), "rows", len(diff.Peers),
		"appeared", diff.Appeared, "moved", diff.Moved, "vanished", diff.Vanished, "silent", diff.Silent)
	return nil
}

// RunDaemon cycles forever until ctx is cancelled.
func (p *Poller) RunDaemon(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollEvery)
	defer ticker.Stop()
	for {
		if err := p.CycleOnce(ctx); err != nil {
			p.log.Error("cycle failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// StatsSnapshot returns a copy of the health stats.
func (p *Poller) StatsSnapshot() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.UptimeS = int64(time.Since(p.started).Seconds())
	s.NodeKey = p.nodeKey
	return s
}

func (p *Poller) refreshList(ctx context.Context) error {
	if p.list != nil && time.Since(p.listAt) < p.cfg.ListTTL {
		return nil
	}
	pn, err := FetchPublicNodes(ctx, p.cfg.PublicNodesURL)
	if err != nil {
		if p.list == nil {
			return p.setErr("fetch publicnodes: " + err.Error())
		}
		p.log.Warn("publicnodes refresh failed, keeping old list", "err", err)
		return nil
	}
	p.pubList = pn.PeersByKey()
	p.pubUp = 0
	for _, lp := range p.pubList {
		if lp.Up {
			p.pubUp++
		}
	}
	p.list, p.listUp = p.pubList, p.pubUp
	p.listAt = time.Now()
	p.log.Info("publicnodes refreshed", "peers", len(p.pubList), "up", p.pubUp)
	return nil
}

// ensurePeerings makes sure the node has at least one live outgoing peering,
// adding a few fresh public peers when needed.
func (p *Poller) ensurePeerings(ctx context.Context) error {
	if p.node.PeersUp() > 0 {
		return nil
	}
	for _, uri := range SelectPeersFromList(p.list, 3, p.addedPeer) {
		if err := p.node.AddPeer(uri); err != nil {
			p.log.Warn("add peer", "uri", uri, "err", err)
			continue
		}
		p.addedPeer[uri] = true
		p.log.Info("added peering", "uri", uri)
	}
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if p.node.PeersUp() > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return p.setErr("no peering is up")
}

// FlushWAL attempts to send all pending WAL batches (used at startup and
// inside every cycle).
func (p *Poller) FlushWAL() error { return p.flushWAL() }

func (p *Poller) flushWAL() error {
	pending, err := p.wal.Pending()
	if err != nil {
		return p.setErr("wal read: " + err.Error())
	}
	for i, b := range pending {
		if err := p.store.Ingest(b); err != nil {
			p.setErrWAL(fmt.Errorf("ingest pending batch %d/%d: %w", i+1, len(pending), err))
			p.setWalPending(len(pending) - i)
			return nil // cycle continues; batch retried next time
		}
	}
	if err := p.wal.Clear(); err != nil {
		return p.setErr("wal clear: " + err.Error())
	}
	p.setWalPending(0)
	return nil
}

func (p *Poller) setWalPending(n int) {
	p.mu.Lock()
	p.stats.WALPending = n
	p.mu.Unlock()
}

func (p *Poller) setErr(msg string) error {
	p.mu.Lock()
	p.stats.LastError = msg
	p.mu.Unlock()
	return fmt.Errorf("%s", msg)
}

func (p *Poller) setErrWAL(err error) {
	p.mu.Lock()
	p.stats.LastError = err.Error()
	p.mu.Unlock()
	p.log.Error("ingest failed", "err", err)
}

func (p *Poller) setErrf(format string, args ...any) error {
	return p.setErr(fmt.Sprintf(format, args...))
}

// SelectPeersFromList picks up to n up tcp/tls peer URIs that are not in the
// exclude set (the node's persistent peers are configured separately).
func SelectPeersFromList(list map[string]*ListPeer, n int, exclude map[string]bool) []string {
	type cand struct {
		uri string
		key string
	}
	var cands []cand
	for k, lp := range list {
		if !lp.Up {
			continue
		}
		for _, uri := range lp.Endpoints {
			if exclude[uri] {
				continue
			}
			scheme := uri
			if i := strings.Index(uri, ":"); i > 0 {
				scheme = uri[:i]
			}
			if scheme != "tcp" && scheme != "tls" {
				continue
			}
			cands = append(cands, cand{uri: uri, key: k})
			break
		}
	}
	var out []string
	seen := map[string]bool{}
	for _, c := range cands {
		if seen[c.key] {
			continue
		}
		seen[c.key] = true
		out = append(out, c.uri)
		if len(out) >= n {
			break
		}
	}
	return out
}
