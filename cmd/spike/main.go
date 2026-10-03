// Command spike validates the project's core polling method (stage 0):
//
//  1. A single embedded 0.5.x node peers with a few public peers.
//  2. Coordinates are NOT flooded for the whole network — a node learns a
//     peer's path only by triggering a lookup, so we PROBE every keyed
//     public peer with a one-byte packet and wait for signed path
//     notifications.
//  3. Live peers answer with coordinates; dead peers stay silent — which is
//     exactly the liveness signal we want to record.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"ymonitor/internal/collector"
	"ymonitor/internal/ygg"
)

func main() {
	peersFlag := flag.String("peers", "", "comma-separated peer URIs (default: auto-select from publicnodes)")
	probeWait := flag.Duration("probe-wait", 150*time.Second, "max time to wait for path answers")
	quiet := flag.Duration("quiet", 20*time.Second, "stop early after this much silence")
	dataDir := flag.String("data-dir", ".spike", "directory for the node identity key")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	pn, err := collector.FetchPublicNodes(context.Background(), "")
	must(err, "fetch publicnodes")

	byKey := pn.PeersByKey()
	upCount := 0
	for _, p := range byKey {
		if p.Up {
			upCount++
		}
	}
	fmt.Printf("publicnodes: %d distinct keyed peers (%d up)\n", len(byKey), upCount)

	var peers []string
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}
	} else {
		peers = pn.SelectPeers(5)
	}
	if len(peers) == 0 {
		fatal("no up tcp/tls peers found in publicnodes")
	}
	fmt.Println("peering with:")
	for _, p := range peers {
		fmt.Printf("  %s\n", p)
	}

	node, err := ygg.New(*dataDir, peers, ygg.Slogger{L: logger})
	must(err, "start node")
	defer node.Stop()
	fmt.Printf("monitor node key: %s\n", node.PublicKey())

	waitPeers(node, 60*time.Second)
	fmt.Println("settling 15s …")
	time.Sleep(15 * time.Second)

	// Probe phase: one packet per keyed peer, paced; count answers.
	var notified atomic.Int64
	node.SetPathNotify(func(ed25519.PublicKey) { notified.Add(1) })

	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	start := time.Now()
	go func() {
		for i, k := range keys {
			raw, err := hex.DecodeString(k)
			if err != nil || len(raw) != ed25519.PublicKeySize {
				continue
			}
			if err := node.Probe(ed25519.PublicKey(raw)); err != nil {
				fmt.Printf("probe %s: %v\n", k[:12], err)
			}
			if i%20 == 19 {
				fmt.Printf("probed %d/%d (answers so far: %d)\n", i+1, len(keys), notified.Load())
			}
			time.Sleep(50 * time.Millisecond) // ~20/s
		}
		fmt.Printf("probed all %d keys in %.0fs\n", len(keys), time.Since(start).Seconds())
	}()

	lastCount := int64(-1)
	since := time.Now()
	deadline := time.Now().Add(*probeWait)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		c := notified.Load()
		fmt.Printf("  answers: %d\n", c)
		if c == lastCount && time.Since(since) >= *quiet {
			break
		}
		if c != lastCount {
			lastCount, since = c, time.Now()
		}
	}

	// Collect results.
	paths := node.Paths()
	pathCoords := map[string]string{}
	for _, p := range paths {
		pathCoords[hex.EncodeToString(p.Key)] = collector.PathString(p.Path)
	}
	tree := node.Tree()
	fmt.Println("\n=== results ===")
	fmt.Printf("tree entries: %d, path entries: %d\n", len(tree), len(paths))

	var upHas, upMiss, downHas, downMiss int
	for k, p := range byKey {
		_, ok := pathCoords[k]
		switch {
		case p.Up && ok:
			upHas++
		case p.Up:
			upMiss++
		case ok:
			downHas++
		default:
			downMiss++
		}
	}
	fmt.Printf("UP peers   (per crawler): answered %d, silent %d\n", upHas, upMiss)
	fmt.Printf("DOWN peers (per crawler): answered %d, silent %d\n", downHas, downMiss)

	fmt.Println("\nsample coords (up peers):")
	n := 0
	for _, k := range keys {
		if !byKey[k].Up {
			continue
		}
		if c, ok := pathCoords[k]; ok {
			fmt.Printf("  %s… -> %-16s %s\n", k[:12], c, byKey[k].IPv6)
			if n++; n >= 8 {
				break
			}
		}
	}

	fmt.Print("\nverdict: ")
	switch {
	case upHas+upMiss == 0:
		fmt.Println("FAIL — no up peers to compare")
		os.Exit(1)
	case upHas >= (upHas+upMiss)*90/100:
		fmt.Printf("PASS — probes learn coordinates for %d/%d up peers; silent ones are plausibly dead (%d down peers answered — crawler lag)\n", upHas, upHas+upMiss, downHas)
	default:
		fmt.Printf("PARTIAL — only %d/%d up peers answered\n", upHas, upHas+upMiss)
		os.Exit(1)
	}
}

func waitPeers(node *ygg.Node, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range node.Peers() {
			if p.Up {
				fmt.Printf("peering up: %s\n", p.URI)
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	fatal("no peering came up within " + timeout.String())
}

func must(err error, what string) {
	if err != nil {
		fatal(what + ": " + err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "spike: "+msg)
	os.Exit(1)
}
