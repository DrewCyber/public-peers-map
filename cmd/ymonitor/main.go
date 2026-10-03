// ymonitor — Yggdrasil public peers topology monitor.
//
// A single static binary with an embedded headless Yggdrasil node (no TUN).
// Every cycle it probes each public peer to learn its current tree
// coordinates and pushes changed rows + events to the ymonitor-api worker.
//
// Modes:
//
//	ymonitor          daemon: cycles every POLL_INTERVAL, local health server
//	ymonitor -once    one cycle (wait for peering, sample, ingest, exit)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ymonitor/internal/collector"
	"ymonitor/internal/store"
	"ymonitor/internal/ygg"
)

func main() {
	cfg := parseConfig()

	level := slog.LevelInfo
	switch strings.ToLower(cfg.logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	if cfg.workerURL == "" || cfg.ingestToken == "" {
		fatal("WORKER_URL and INGEST_TOKEN are required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if cfg.once {
		ctx, cancel = context.WithTimeout(ctx, cfg.onceTimeout)
		defer cancel()
	}

	// Resolve our own peerings: explicit list, or auto-select from the list.
	peerURIs := cfg.peers
	if len(peerURIs) == 0 {
		if pn, err := collector.FetchPublicNodes(ctx, cfg.publicNodesURL); err != nil {
			log.Warn("cannot fetch publicnodes for auto-peering; will retry in-cycle", "err", err)
		} else {
			peerURIs = pn.SelectPeers(5)
		}
	}
	if len(peerURIs) > 0 {
		log.Info("own peerings", "uris", strings.Join(peerURIs, ", "))
	}

	node, err := ygg.New(cfg.dataDir, peerURIs, ygg.Slogger{L: slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	if err != nil {
		fatal("node: " + err.Error())
	}
	defer node.Stop()
	log.Info("monitor node started", "key", node.PublicKey())

	wal, err := store.OpenWAL(cfg.dataDir)
	if err != nil {
		fatal("wal: " + err.Error())
	}
	api := store.NewClient(cfg.workerURL, cfg.ingestToken)

	poller := collector.NewPoller(node, api, wal, collector.PollerConfig{
		PollEvery:      cfg.pollEvery,
		ListTTL:        cfg.listTTL,
		Probe:          collector.ProbeOptions{Pace: cfg.probePace, Quiet: cfg.probeQuiet, Timeout: cfg.probeTimeout},
		VanishAfter:    cfg.vanishAfter,
		HeartbeatEvery: cfg.heartbeatEvery,
		PublicNodesURL: cfg.publicNodesURL,
	}, log)

	// Flush anything left from a previous run before collecting new data.
	if err := poller.FlushWAL(); err != nil {
		log.Error("startup WAL flush", "err", err)
	}

	if cfg.once {
		if err := poller.CycleOnce(ctx); err != nil {
			log.Error("cycle failed", "err", err)
			node.Stop()
			os.Exit(1)
		}
		log.Info("once-mode cycle complete")
		node.Stop()
		return
	}

	if cfg.healthAddr != "" {
		go serveHealth(cfg.healthAddr, poller, log)
	}

	poller.RunDaemon(ctx)
	node.Stop()
}

func serveHealth(addr string, poller *collector.Poller, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(poller.StatsSnapshot())
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("health server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Error("health server", "err", err)
	}
}

// ---------- configuration ----------

type config struct {
	once           bool
	onceTimeout    time.Duration
	workerURL      string
	ingestToken    string
	peers          []string
	pollEvery      time.Duration
	listTTL        time.Duration
	dataDir        string
	publicNodesURL string
	vanishAfter    time.Duration
	heartbeatEvery time.Duration
	probePace      time.Duration
	probeQuiet     time.Duration
	probeTimeout   time.Duration
	healthAddr     string
	logLevel       string
}

func parseConfig() config {
	var c config
	fs := flag.NewFlagSet("ymonitor", flag.ExitOnError)
	fs.BoolVar(&c.once, "once", envBool("ONCE", false), "run a single cycle and exit (env ONCE)")
	fs.DurationVar(&c.onceTimeout, "once-timeout", envDuration("ONCE_TIMEOUT", 10*time.Minute), "overall timeout for -once")
	fs.StringVar(&c.workerURL, "worker-url", envString("WORKER_URL", ""), "ymonitor-api worker URL (env WORKER_URL)")
	fs.StringVar(&c.ingestToken, "ingest-token", envString("INGEST_TOKEN", ""), "ingest bearer token (env INGEST_TOKEN)")
	fs.Var(&csvFlag{&c.peers}, "peers", "comma-separated own peer URIs (env PEERS; default: auto-select)")
	fs.DurationVar(&c.pollEvery, "interval", envDuration("POLL_INTERVAL", 5*time.Minute), "poll cycle interval; also sets the vanish window default (env POLL_INTERVAL)")
	fs.DurationVar(&c.listTTL, "list-ttl", envDuration("LIST_TTL", 24*time.Hour), "publicnodes.json refresh interval (env LIST_TTL)")
	fs.StringVar(&c.dataDir, "data-dir", envString("DATA_DIR", "./data"), "dir for node key and WAL (env DATA_DIR)")
	fs.StringVar(&c.publicNodesURL, "publicnodes-url", envString("PUBLICNODES_URL", collector.PublicNodesURL), "public peers list URL")
	fs.DurationVar(&c.vanishAfter, "vanish-after", envDuration("VANISH_AFTER", 0), "mark a present peer vanished after this much silence; 0 = 3x interval (env VANISH_AFTER)")
	fs.DurationVar(&c.heartbeatEvery, "heartbeat-every", envDuration("HEARTBEAT_EVERY", 15*time.Minute), "rewrite stable peers' rows at least this often to refresh last_answer_ts (env HEARTBEAT_EVERY)")
	fs.DurationVar(&c.probePace, "probe-pace", envDuration("PROBE_PACE", 50*time.Millisecond), "delay between probes (env PROBE_PACE)")
	fs.DurationVar(&c.probeQuiet, "probe-quiet", envDuration("PROBE_QUIET", 20*time.Second), "stop sampling after this much silence (env PROBE_QUIET)")
	fs.DurationVar(&c.probeTimeout, "probe-timeout", envDuration("PROBE_TIMEOUT", 120*time.Second), "hard cap for one sampling sweep (env PROBE_TIMEOUT)")
	fs.StringVar(&c.healthAddr, "health", envString("HEALTH_ADDR", "127.0.0.1:8080"), "local health endpoint, empty disables (env HEALTH_ADDR)")
	fs.StringVar(&c.logLevel, "log-level", envString("LOG_LEVEL", "info"), "debug|info|warn (env LOG_LEVEL)")
	_ = fs.Parse(os.Args[1:])
	return c
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "1" || v == "true" || v == "yes"
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

type csvFlag struct{ dst *[]string }

func (f csvFlag) String() string {
	if f.dst == nil {
		return ""
	}
	return strings.Join(*f.dst, ",")
}

func (f csvFlag) Set(v string) error {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	*f.dst = out
	return nil
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "ymonitor: "+msg)
	os.Exit(1)
}
