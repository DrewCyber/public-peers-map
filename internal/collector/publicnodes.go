package collector

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/yggdrasil-network/yggdrasil-go/src/address"
)

// PublicNodesURL is the canonical public peers list, refreshed by its
// crawler every ~5 minutes. Entries already carry the peer's public key.
const PublicNodesURL = "https://publicpeers.neilalexander.dev/publicnodes.json"

// PublicNodes mirrors publicnodes.json: markdown source file -> peer URI -> entry.
type PublicNodes map[string]map[string]PublicNodeEntry

type PublicNodeEntry struct {
	Up         bool   `json:"up"`
	Key        string `json:"key"`
	Imported   int64  `json:"imported"`
	Updated    int64  `json:"updated"`
	LastSeen   int64  `json:"last_seen"`
	ProtoMinor int    `json:"proto_minor"`
	ResponseMS int    `json:"response_ms"`
}

// ListPeer is one public peer: a node key with all of its advertised URIs.
type ListPeer struct {
	Key       string   // ed25519 public key, hex
	IPv6      string   // 200::/7 address derived from the key
	Endpoints []string // peer URIs, e.g. tcp://1.2.3.4:1337
	Up        bool     // at least one endpoint was up at last crawler check
	Country   string   // from the public-peers file the peer is listed in, e.g. "Hong Kong"
}

func FetchPublicNodes(ctx context.Context, url string) (PublicNodes, error) {
	if url == "" {
		url = PublicNodesURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "ymonitor (+https://github.com/DrewCyber/public-peers-map)")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("publicnodes: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var pn PublicNodes
	if err := json.Unmarshal(body, &pn); err != nil {
		return nil, fmt.Errorf("publicnodes: parse: %w", err)
	}
	return pn, nil
}

// PeersByKey collapses all URIs that share a node key into one ListPeer.
// Peers without a known key (never seen up) are skipped. Country comes from
// the public-peers markdown file name; the rare peer listed in several
// countries gets the alphabetically first one (deterministic).
func (pn PublicNodes) PeersByKey() map[string]*ListPeer {
	byKey := map[string]*ListPeer{}
	for file, entries := range pn {
		country := CountryFromFile(file)
		for uri, e := range entries {
			if e.Key == "" {
				continue
			}
			p, ok := byKey[e.Key]
			if !ok {
				p = &ListPeer{Key: e.Key, Country: country}
				byKey[e.Key] = p
			}
			if country != "" && (p.Country == "" || country < p.Country) {
				p.Country = country
			}
			p.Endpoints = append(p.Endpoints, uri)
			if e.Up {
				p.Up = true
			}
		}
	}
	for _, p := range byKey {
		sort.Strings(p.Endpoints)
		if ipv6, err := IPv6ForKey(p.Key); err == nil {
			p.IPv6 = ipv6
		}
	}
	return byKey
}

// CountryFromFile turns a public-peers file name into a display country:
// "hong-kong.md" -> "Hong Kong".
func CountryFromFile(file string) string {
	name := strings.TrimSuffix(file, ".md")
	name = strings.NewReplacer("-", " ", "_", " ").Replace(name)
	words := strings.Fields(name)
	for i, w := range words {
		words[i] = strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
	}
	return strings.Join(words, " ")
}

// SelectPeers returns up to n URIs of distinct, currently-up 0.5.x peers,
// fastest first, preferring plain tcp links. Used to pick the monitor
// node's own peerings.
func (pn PublicNodes) SelectPeers(n int) []string {
	type cand struct {
		uri   string
		key   string
		proto string
		ms    int
	}
	var cands []cand
	for _, entries := range pn {
		for uri, e := range entries {
			if !e.Up || e.Key == "" || e.ProtoMinor < 5 {
				continue
			}
			scheme := uri
			if i := strings.Index(uri, "://"); i > 0 {
				scheme = uri[:i]
			}
			if scheme != "tcp" && scheme != "tls" {
				continue
			}
			cands = append(cands, cand{uri: uri, key: e.Key, proto: scheme, ms: e.ResponseMS})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].proto != cands[j].proto {
			return cands[i].proto == "tcp" // tcp before tls
		}
		return cands[i].ms < cands[j].ms
	})
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

// IPv6ForKey derives the node's 200::/7 address from its hex ed25519 public
// key, using yggdrasil's own address package (the same code path as
// core.Address()).
func IPv6ForKey(keyHex string) (string, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(keyHex))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return "", fmt.Errorf("bad public key %q", keyHex)
	}
	addr := address.AddrForKey(ed25519.PublicKey(raw))
	return net.IP(addr[:]).String(), nil
}
