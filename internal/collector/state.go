package collector

import (
	"encoding/json"
	"strings"
	"time"
)

// PeerRow is the per-peer state stored in D1 (one row per public node).
// UpdatedAt is set by the worker (from poll_ts) on writes and populated on
// reads; the collector never sends it.
type PeerRow struct {
	Key          string   `json:"key"`            // ed25519 public key, hex
	IPv6         string   `json:"ipv6"`           // 200::/7 address
	Endpoints    []string `json:"endpoints"`      // peer URIs
	Coords       *string  `json:"coords"`         // "1.2.3", "" = root node, null = not in tree
	LastAnswerTS *int64   `json:"last_answer_ts"` // last poll where the peer answered, null = never
	Country      string   `json:"country,omitempty"`
	UpdatedAt    int64    `json:"updated_at,omitempty"`
}

// Event records one observed coordinates transition. NULL in OldCoords means
// the peer appeared in the tree; NULL in NewCoords means it disappeared.
type Event struct {
	TS        int64   `json:"ts"`
	PeerKey   string  `json:"peer_key"`
	OldCoords *string `json:"old_coords"`
	NewCoords *string `json:"new_coords"`
}

// Batch is the payload POSTed to the worker's /v1/ingest.
type Batch struct {
	PollTS int64     `json:"poll_ts"`
	Peers  []PeerRow `json:"peers,omitempty"`
	Events []Event   `json:"events,omitempty"`
}

// Store is what the poller needs from the API side (implemented by
// store.Client; kept as an interface to avoid an import cycle).
type Store interface {
	Ingest(batch Batch) error
	FetchPeers() ([]PeerRow, error)
}

// WALStore is the write-ahead log contract (implemented by store.WAL).
type WALStore interface {
	Append(batch Batch) error
	Pending() ([]Batch, error)
	Clear() error
}

// trackState is the poller's in-memory view of one peer between cycles.
// LastAnswer/LastAnswerDB persist through the DB (last_answer_ts), so the
// vanish debounce works statelessly across -once runs and hosts.
type trackState struct {
	Coords    string // last confirmed coords; valid iff Present
	Present   bool   // currently considered present in the tree
	Endpoints string // joined endpoints, for change detection
	IPv6      string
	Country   string
	Known     bool  // a row for this peer was already emitted
	LastAnswerDB int64 // last_answer_ts value as last written to the DB row
	LastAnswer   int64 // ts of the most recent cycle where the peer answered
}

// Tracker carries per-peer state across poll cycles.
type Tracker struct {
	m map[string]*trackState
}

func NewTracker() *Tracker {
	return &Tracker{m: map[string]*trackState{}}
}

// KeyForEndpoint returns the key of the tracked peer that advertises uri as
// one of its endpoints, or "". Custom peers use this to recover their key
// from stored rows when the peer itself is currently down.
func (t *Tracker) KeyForEndpoint(uri string) string {
	if t == nil {
		return ""
	}
	for key, st := range t.m {
		for _, ep := range strings.Split(st.Endpoints, "\x00") {
			if ep == uri {
				return key
			}
		}
	}
	return ""
}

// Bootstrap fills the tracker from the worker's current /v1/peers state so
// that diffs (and the stateless vanish debounce) continue from stored truth.
func (t *Tracker) Bootstrap(rows []PeerRow) {
	for _, r := range rows {
		st := &trackState{
			Endpoints: strings.Join(r.Endpoints, "\x00"),
			IPv6:      r.IPv6,
			Country:   r.Country,
			Known:     true,
		}
		if r.Coords != nil {
			st.Present = true
			st.Coords = *r.Coords
		}
		if r.LastAnswerTS != nil {
			st.LastAnswer, st.LastAnswerDB = *r.LastAnswerTS, *r.LastAnswerTS
		} else if st.Present {
			// Defensive: a present row without an answer timestamp counts as
			// answered at its last write time.
			st.LastAnswer, st.LastAnswerDB = r.UpdatedAt, r.UpdatedAt
		}
		t.m[r.Key] = st
	}
}

// DiffOptions controls one diff run.
type DiffOptions struct {
	Now            int64         // unix ts stamped on emitted rows/events
	VanishWindow   time.Duration // present but silent longer than this -> vanish
	HeartbeatEvery time.Duration // rewrite rows of stable peers at least this often
}

// DiffResult is what a cycle wants to write.
type DiffResult struct {
	Peers   []PeerRow
	Events  []Event
	Appeared int
	Moved    int
	Vanished int
	Silent   int // present peers currently silent but within the vanish window
}

// Diff compares a fresh coordinates sample against the tracker and returns
// the rows to upsert and the events to log. list is the current publicnodes
// peer set; sample maps key -> coords for every peer that answered.
//
// The vanish decision is purely time-based: a present peer that has not
// answered for longer than VanishWindow is marked vanished. This works with
// any number of independent collectors, since "last answered" lives in the
// database, not in process memory.
func Diff(list map[string]*ListPeer, sample map[string]string, tr *Tracker, opts DiffOptions) DiffResult {
	heartbeatSec := int64(opts.HeartbeatEvery / time.Second)
	vanishSec := int64(opts.VanishWindow / time.Second)
	var res DiffResult

	for key, lp := range list {
		st, ok := tr.m[key]
		if !ok {
			st = &trackState{}
			tr.m[key] = st
		}

		rowChanged := !st.Known
		answered := false
		if coords, inSample := sample[key]; inSample {
			answered = true
			st.LastAnswer = opts.Now
			if !st.Present {
				res.Events = append(res.Events, Event{TS: opts.Now, PeerKey: key, OldCoords: nil, NewCoords: strPtr(coords)})
				res.Appeared++
				rowChanged = true
			} else if st.Coords != coords {
				old := st.Coords
				res.Events = append(res.Events, Event{TS: opts.Now, PeerKey: key, OldCoords: strPtr(old), NewCoords: strPtr(coords)})
				res.Moved++
				rowChanged = true
			}
			st.Present, st.Coords = true, coords
		} else if st.Present && st.LastAnswer > 0 && opts.Now-st.LastAnswer > vanishSec {
			old := st.Coords
			res.Events = append(res.Events, Event{TS: opts.Now, PeerKey: key, OldCoords: strPtr(old), NewCoords: nil})
			res.Vanished++
			st.Present = false
			rowChanged = true
		} else if st.Present {
			res.Silent++
		}

		ep := strings.Join(lp.Endpoints, "\x00")
		if ep != st.Endpoints || lp.IPv6 != st.IPv6 || lp.Country != st.Country {
			rowChanged = true
		}
		// Heartbeat: periodically rewrite rows of stable, answering peers so
		// that last_answer_ts stays fresh for other collectors.
		if answered && heartbeatSec > 0 && opts.Now-st.LastAnswerDB >= heartbeatSec {
			rowChanged = true
		}

		st.Endpoints, st.IPv6, st.Country, st.Known = ep, lp.IPv6, lp.Country, true

		if rowChanged {
			st.LastAnswerDB = st.LastAnswer
			row := PeerRow{
				Key:       key,
				IPv6:      lp.IPv6,
				Endpoints: append([]string(nil), lp.Endpoints...),
				Country:   lp.Country,
			}
			if st.Present {
				row.Coords = strPtr(st.Coords)
			}
			if st.LastAnswer > 0 {
				ts := st.LastAnswer
				row.LastAnswerTS = &ts
			}
			res.Peers = append(res.Peers, row)
		}
	}
	return res
}

// MarshalJSONLine renders a batch as one WAL line.
func (b Batch) MarshalJSONLine() ([]byte, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func strPtr(s string) *string { return &s }
