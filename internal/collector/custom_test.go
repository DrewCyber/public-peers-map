package collector

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func TestNormalizePeerURI(t *testing.T) {
	cases := []struct{ in, want string }{
		{"tcp://opns.nixm.ru:9002", "tcp://opns.nixm.ru:9002"},
		{"TCP://OPNS.NixM.RU:9002", "tcp://opns.nixm.ru:9002"},
		{"tcp://opns.nixm.ru:9002?priority=5", "tcp://opns.nixm.ru:9002"},
		{" tcp://host:1 ", "tcp://host:1"},
		{"not a uri", "not%20a%20uri"}, // lenient parse + re-escape; garbage in, garbage out
	}
	for _, c := range cases {
		if got := normalizePeerURI(c.in); got != c.want {
			t.Errorf("normalizePeerURI(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestResolveCustomPeers(t *testing.T) {
	_, livePriv, _ := ed25519.GenerateKey(rand.Reader)
	_, downPriv, _ := ed25519.GenerateKey(rand.Reader)
	liveKey := hex.EncodeToString(livePriv.Public().(ed25519.PublicKey))
	downKey := hex.EncodeToString(downPriv.Public().(ed25519.PublicKey))

	tr := NewTracker()
	tr.Bootstrap([]PeerRow{{
		Key:       downKey,
		IPv6:      "200:1111:1111:1111:1111:1111:1111:1111",
		Endpoints: []string{"tcp://down.example:9000"},
	}})

	links := []LinkInfo{
		{URI: "tcp://live.example:9001", Up: true, Inbound: false, Key: liveKey},
		{URI: "tcp://down.example:9000", Up: false, Inbound: false, Key: ""},           // link down -> tracker fallback
		{URI: "tcp://inbound.example:9", Up: true, Inbound: true, Key: liveKey},        // inbound never matches
		{URI: "TCP://Case.Example:9002?priority=1", Up: true, Inbound: false, Key: ""}, // up but keyless
	}
	got := ResolveCustomPeers([]string{
		"tcp://live.example:9001",
		"tcp://down.example:9000",
		"tcp://never-seen.example:9003",
		"tcp://case.example:9002",
	}, links, tr)

	byURI := map[string]CustomPeer{}
	for _, c := range got {
		byURI[c.URI] = c
	}
	if c := byURI["tcp://live.example:9001"]; c.Key != liveKey || !c.Up {
		t.Errorf("live peer: got %+v", c)
	}
	if c := byURI["tcp://down.example:9000"]; c.Key != downKey || c.Up {
		t.Errorf("down peer should recover key from tracker, not be Up: got %+v", c)
	}
	if c := byURI["tcp://never-seen.example:9003"]; c.Key != "" {
		t.Errorf("unknown peer should have empty key: got %+v", c)
	}
	if c := byURI["tcp://case.example:9002"]; c.Key != "" {
		t.Errorf("up-but-keyless link must not resolve: got %+v", c)
	}
}

func TestMergeCustomInto(t *testing.T) {
	_, pubPriv, _ := ed25519.GenerateKey(rand.Reader)
	_, customPriv, _ := ed25519.GenerateKey(rand.Reader)
	pubKey := hex.EncodeToString(pubPriv.Public().(ed25519.PublicKey))
	customKey := hex.EncodeToString(customPriv.Public().(ed25519.PublicKey))

	list := map[string]*ListPeer{
		pubKey: {Key: pubKey, Endpoints: []string{"tcp://pub.example:1"}, Up: true},
	}
	up := MergeCustomInto(list, []CustomPeer{
		{URI: "tcp://custom-up.example:2", Key: customKey, Up: true},
		{URI: "tcp://custom-unknown.example:3", Key: ""},                       // skipped
		{URI: "tcp://now-public.example:4", Key: pubKey, Up: false},            // publicnodes wins
		{URI: "tcp://custom-badkey.example:5", Key: "not-hex"},                 // skipped
	})
	if up != 1 {
		t.Errorf("up = %d, want 1", up)
	}
	if len(list) != 2 {
		t.Fatalf("list has %d peers, want 2 (pub + one custom)", len(list))
	}
	lp := list[customKey]
	if lp == nil {
		t.Fatal("custom peer not merged")
	}
	if !lp.Up || lp.Endpoints[0] != "tcp://custom-up.example:2" || lp.IPv6 == "" {
		t.Errorf("merged entry wrong: %+v", lp)
	}
	if lp.Country != "" {
		t.Errorf("custom peer should carry no country, got %q", lp.Country)
	}
}

func TestTrackerKeyForEndpoint(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	key := hex.EncodeToString(priv.Public().(ed25519.PublicKey))
	tr := NewTracker()
	tr.Bootstrap([]PeerRow{{
		Key:       key,
		Endpoints: []string{"tcp://a.example:1", "tls://b.example:2"},
	}})
	if got := tr.KeyForEndpoint("tls://b.example:2"); got != key {
		t.Errorf("KeyForEndpoint = %q, want %q", got, key)
	}
	if got := tr.KeyForEndpoint("tcp://c.example:3"); got != "" {
		t.Errorf("unknown endpoint should give \"\", got %q", got)
	}
	var nilTr *Tracker
	if got := nilTr.KeyForEndpoint("tcp://a.example:1"); got != "" {
		t.Errorf("nil tracker should give \"\", got %q", got)
	}
}
