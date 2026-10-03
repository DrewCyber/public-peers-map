package collector

import (
	"strings"
	"testing"
	"time"
)

func lp(key string, endpoints ...string) *ListPeer {
	return &ListPeer{Key: key, IPv6: "200:dead:beef::1", Endpoints: endpoints, Up: true}
}

func coordsOf(rows []PeerRow, key string) *string {
	for _, r := range rows {
		if r.Key == key {
			return r.Coords
		}
	}
	return nil
}

// opts with a 15-minute vanish window and a 1-hour heartbeat.
func optsAt(now int64) DiffOptions {
	return DiffOptions{Now: now, VanishWindow: 15 * time.Minute, HeartbeatEvery: time.Hour}
}

func TestDiffFreshList(t *testing.T) {
	tr := NewTracker()
	list := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1"), "bb": lp("bb", "tcp://2.2.2.2:2")}
	sample := map[string]string{"aa": "1.2"}

	res := Diff(list, sample, tr, optsAt(1000))

	if res.Appeared != 1 || res.Moved != 0 || res.Vanished != 0 {
		t.Fatalf("counters: %+v", res)
	}
	if len(res.Events) != 1 || res.Events[0].PeerKey != "aa" ||
		res.Events[0].OldCoords != nil || *res.Events[0].NewCoords != "1.2" {
		t.Fatalf("events: %+v", res.Events)
	}
	// both rows emitted (bb registered with null coords), aa present
	if len(res.Peers) != 2 {
		t.Fatalf("rows: %+v", res.Peers)
	}
	if c := coordsOf(res.Peers, "aa"); c == nil || *c != "1.2" {
		t.Errorf("aa coords: %v", c)
	}
	if c := coordsOf(res.Peers, "bb"); c != nil {
		t.Errorf("bb coords must be null, got %v", *c)
	}
	// aa answered -> last_answer_ts set; bb never answered -> null
	for _, r := range res.Peers {
		if r.Key == "aa" && (r.LastAnswerTS == nil || *r.LastAnswerTS != 1000) {
			t.Errorf("aa last_answer_ts: %v", r.LastAnswerTS)
		}
		if r.Key == "bb" && r.LastAnswerTS != nil {
			t.Errorf("bb last_answer_ts must be nil, got %v", *r.LastAnswerTS)
		}
	}
}

func TestDiffMoveAndTimeBasedVanish(t *testing.T) {
	tr := NewTracker()
	list := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1")}
	const t0 = int64(1000000)

	// appear at t0
	res := Diff(list, map[string]string{"aa": "1.2"}, tr, optsAt(t0))
	if res.Appeared != 1 {
		t.Fatalf("appear: %+v", res)
	}

	// move at t0+5m
	res = Diff(list, map[string]string{"aa": "5.6"}, tr, optsAt(t0+5*60))
	if res.Moved != 1 || *res.Events[0].OldCoords != "1.2" || *res.Events[0].NewCoords != "5.6" {
		t.Fatalf("move: %+v", res.Events)
	}

	// silent 10m after last answer: still within the 15m window
	res = Diff(list, map[string]string{}, tr, optsAt(t0+5*60+10*60))
	if res.Vanished != 0 || len(res.Events) != 0 || res.Silent != 1 {
		t.Fatalf("silent within window: %+v", res)
	}

	// silent 16m after last answer: vanish + row with null coords
	res = Diff(list, map[string]string{}, tr, optsAt(t0+5*60+16*60))
	if res.Vanished != 1 || *res.Events[0].OldCoords != "5.6" || res.Events[0].NewCoords != nil {
		t.Fatalf("vanish: %+v", res.Events)
	}
	if c := coordsOf(res.Peers, "aa"); c != nil {
		t.Errorf("vanish row coords must be null, got %v", *c)
	}

	// answer again: re-appear
	res = Diff(list, map[string]string{"aa": "9"}, tr, optsAt(t0+30*60))
	if res.Appeared != 1 || *res.Events[0].NewCoords != "9" {
		t.Fatalf("re-appear: %+v", res.Events)
	}

	// a fresh answer keeps it alive: answered at t0+40m, silent at t0+50m -> no vanish
	Diff(list, map[string]string{"aa": "9"}, tr, optsAt(t0+40*60))
	res = Diff(list, map[string]string{}, tr, optsAt(t0+50*60))
	if res.Vanished != 0 || res.Silent != 1 {
		t.Fatalf("fresh answer must reset the window: %+v", res)
	}
}

func TestDiffVanishAcrossProcesses(t *testing.T) {
	// A -once process bootstraps from DB state: the peer answered long ago,
	// has not answered since beyond the window -> vanish fires statelessly.
	old := int64(1000)
	tr := NewTracker()
	coords := "1.2"
	tr.Bootstrap([]PeerRow{
		{Key: "aa", IPv6: "200:dead:beef::1", Endpoints: []string{"tcp://1.1.1.1:1"}, Coords: &coords, LastAnswerTS: &old},
	})
	list := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1")}

	res := Diff(list, map[string]string{}, tr, optsAt(old+20*60))
	if res.Vanished != 1 || res.Silent != 0 {
		t.Fatalf("stateless vanish: %+v", res)
	}

	// ...but a recent answer protects it
	tr2 := NewTracker()
	recent := old + 10*60
	tr2.Bootstrap([]PeerRow{
		{Key: "aa", IPv6: "200:dead:beef::1", Endpoints: []string{"tcp://1.1.1.1:1"}, Coords: &coords, LastAnswerTS: &recent},
	})
	res = Diff(list, map[string]string{}, tr2, optsAt(old+20*60))
	if res.Vanished != 0 || res.Silent != 1 {
		t.Fatalf("recent answer must protect: %+v", res)
	}
}

func TestDiffHeartbeat(t *testing.T) {
	tr := NewTracker()
	list := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1")}
	const t0 = int64(1000000)

	// heartbeat granularity smaller than window; use dedicated opts
	o := DiffOptions{Now: t0, VanishWindow: time.Hour, HeartbeatEvery: 15 * time.Minute}

	// first cycle: row emitted (new peer)
	res := Diff(list, map[string]string{"aa": "1.2"}, tr, o)
	if len(res.Peers) != 1 {
		t.Fatalf("first cycle rows: %+v", res.Peers)
	}

	// answering with unchanged coords 10m later: no heartbeat yet -> no row
	o.Now = t0 + 10*60
	res = Diff(list, map[string]string{"aa": "1.2"}, tr, o)
	if len(res.Peers) != 0 {
		t.Fatalf("no heartbeat before 15m: %+v", res.Peers)
	}

	// 16m later, still answering and unchanged: heartbeat row refreshes last_answer_ts
	o.Now = t0 + 16*60
	res = Diff(list, map[string]string{"aa": "1.2"}, tr, o)
	if len(res.Peers) != 1 || res.Peers[0].LastAnswerTS == nil || *res.Peers[0].LastAnswerTS != t0+16*60 {
		t.Fatalf("heartbeat row: %+v", res.Peers)
	}
}

func TestDiffEndpointsChangeOnlyRow(t *testing.T) {
	tr := NewTracker()
	list := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1")}
	opts := DiffOptions{Now: 1000, VanishWindow: time.Hour, HeartbeatEvery: time.Hour}
	Diff(list, map[string]string{"aa": "1.2"}, tr, opts)

	// same coords, new endpoint -> row emitted, no event
	list2 := map[string]*ListPeer{"aa": lp("aa", "tcp://1.1.1.1:1", "tls://1.1.1.1:2")}
	res := Diff(list2, map[string]string{"aa": "1.2"}, tr, opts)
	if len(res.Events) != 0 || len(res.Peers) != 1 || len(res.Peers[0].Endpoints) != 2 {
		t.Fatalf("endpoints change: %+v / %+v", res.Events, res.Peers)
	}

	// nothing changed at all -> nothing emitted (heartbeat is 1h away)
	res = Diff(list2, map[string]string{"aa": "1.2"}, tr, opts)
	if len(res.Events) != 0 || len(res.Peers) != 0 {
		t.Fatalf("no-change cycle must emit nothing: %+v / %+v", res.Events, res.Peers)
	}
}

func TestBootstrapFromRows(t *testing.T) {
	c1 := "1.2"
	tr := NewTracker()
	ts := int64(500)
	tr.Bootstrap([]PeerRow{
		{Key: "aa", IPv6: "200::1", Endpoints: []string{"tcp://1.1.1.1:1"}, Coords: &c1, LastAnswerTS: &ts},
		{Key: "bb", IPv6: "200:dead:beef::1", Endpoints: []string{"tcp://2.2.2.2:2"}, Coords: nil},
	})
	list := map[string]*ListPeer{
		"aa": {Key: "aa", IPv6: "200::1", Endpoints: []string{"tcp://1.1.1.1:1"}, Up: true},
		"bb": lp("bb", "tcp://2.2.2.2:2"),
	}

	// same coords for aa, still no answer for bb: nothing happens
	res := Diff(list, map[string]string{"aa": "1.2"}, tr, DiffOptions{Now: 1000, VanishWindow: time.Hour, HeartbeatEvery: time.Hour})
	if len(res.Events) != 0 || len(res.Peers) != 0 {
		t.Fatalf("bootstrapped no-op cycle: %+v / %+v", res.Events, res.Peers)
	}

	// move after bootstrap produces old coords from the DB
	res = Diff(list, map[string]string{"aa": "8"}, tr, DiffOptions{Now: 1000, VanishWindow: time.Hour, HeartbeatEvery: time.Hour})
	if res.Moved != 1 || *res.Events[0].OldCoords != "1.2" {
		t.Fatalf("move after bootstrap: %+v", res.Events)
	}
}

func TestBatchJSON(t *testing.T) {
	c := "1.2"
	b := Batch{PollTS: 42, Peers: []PeerRow{{Key: "k", IPv6: "200::1", Endpoints: []string{"tcp://1.1.1.1:1"}, Coords: &c}},
		Events: []Event{{TS: 42, PeerKey: "k", OldCoords: nil, NewCoords: &c}}}
	line, err := b.MarshalJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(line), "\n") {
		t.Error("line must end with newline")
	}
	if !strings.Contains(string(line), `"old_coords":null`) {
		t.Errorf("json: %s", line)
	}
}
