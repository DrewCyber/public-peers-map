package collector

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"
)

// ProbeNode is the subset of the yggdrasil node the sampler needs (kept as an
// interface for tests).
type ProbeNode interface {
	Probe(key ed25519.PublicKey) error
	SetPathNotify(f func(ed25519.PublicKey))
	PathsMap() map[string]string
}

// ProbeOptions tunes the sampling sweep.
type ProbeOptions struct {
	Pace    time.Duration // delay between individual probes (default 50ms)
	Quiet   time.Duration // stop waiting once answers stop arriving (default 20s)
	Timeout time.Duration // hard cap for the whole sweep (default 120s)
}

func (o ProbeOptions) withDefaults() ProbeOptions {
	if o.Pace <= 0 {
		o.Pace = 50 * time.Millisecond
	}
	if o.Quiet <= 0 {
		o.Quiet = 20 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 120 * time.Second
	}
	return o
}

// SampleCoords probes every key (hex public keys) and returns the coordinates
// of everyone that answered by the time the sweep settles. Coordinates of the
// tree root are the empty string.
func SampleCoords(ctx context.Context, node ProbeNode, keys []string, opts ProbeOptions, log *slog.Logger) map[string]string {
	opts = opts.withDefaults()

	keySet := make(map[string]ed25519.PublicKey, len(keys))
	for _, k := range keys {
		raw, err := hex.DecodeString(k)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			continue
		}
		keySet[k] = ed25519.PublicKey(raw)
	}

	var mu sync.Mutex
answeredMap := map[string]bool{}
	node.SetPathNotify(func(k ed25519.PublicKey) {
		mu.Lock()
		answeredMap[hex.EncodeToString(k)] = true
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		for k, raw := range keySet {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := node.Probe(raw); err != nil {
				log.Debug("probe failed", "key", k[:12], "err", err)
			}
			select {
			case <-time.After(opts.Pace):
			case <-ctx.Done():
				return
			}
		}
	}()

	answered := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for k := range keySet {
			if answeredMap[k] {
				n++
			}
		}
		return n
	}

	total := len(keySet)
	last, since, start := -1, time.Now(), time.Now()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-probeDone:
			// All probes sent; keep waiting a little for stragglers.
			if time.Since(since) >= opts.Quiet {
				break loop
			}
		case <-tick.C:
		}
		n := answered()
		if n != last {
			last, since = n, time.Now()
			log.Info("probe sweep", "answered", n, "total", total, "elapsed", time.Since(start).Round(time.Second))
		}
		if n >= total {
			break loop
		}
		if time.Since(since) >= opts.Quiet {
			break loop
		}
	}

	paths := node.PathsMap()
	out := make(map[string]string, len(paths))
	for k := range keySet {
		if c, ok := paths[k]; ok {
			out[k] = c
		}
	}
	return out
}
