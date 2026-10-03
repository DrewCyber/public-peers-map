package store

import (
	"os"
	"path/filepath"
	"testing"

	"ymonitor/internal/collector"
)

func TestWALRoundtrip(t *testing.T) {
	dir := t.TempDir()
	// OpenWAL creates the directory if missing
	w, err := OpenWAL(filepath.Join(dir, "sub"))
	if err != nil {
		t.Fatal(err)
	}

	pending, err := w.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("empty wal: %v %v", pending, err)
	}

	c1 := "1.2"
	b1 := collector.Batch{PollTS: 1, Peers: []collector.PeerRow{{Key: "a", IPv6: "200::1", Coords: &c1}}}
	b2 := collector.Batch{PollTS: 2, Events: []collector.Event{{TS: 2, PeerKey: "a", NewCoords: &c1}}}
	if err := w.Append(b1); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(b2); err != nil {
		t.Fatal(err)
	}

	pending, err = w.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].PollTS != 1 || pending[1].PollTS != 2 {
		t.Fatalf("pending: %+v", pending)
	}
	if pending[0].Peers[0].Coords == nil || *pending[0].Peers[0].Coords != "1.2" {
		t.Fatalf("roundtrip lost coords: %+v", pending[0].Peers)
	}

	if err := w.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(w.path); !os.IsNotExist(err) {
		t.Fatal("wal file must be removed after clear")
	}
	pending, err = w.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("after clear: %v %v", pending, err)
	}
}

func TestWALTornLineSkipped(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := "5"
	if err := w.Append(collector.Batch{PollTS: 7, Events: []collector.Event{{TS: 7, PeerKey: "a", NewCoords: &c}}}); err != nil {
		t.Fatal(err)
	}
	// simulate a crash mid-append: a torn line lands AFTER the good one
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("{\"poll_ts\":8,\"pe"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	pending, err := w.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].PollTS != 7 {
		t.Fatalf("torn line must be skipped: %+v", pending)
	}
}
