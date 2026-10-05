package collector

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/url"
	"strings"
	"time"
)

// LinkInfo is the subset of the node's link state used to learn custom
// peers' keys (a cut of ygg.Node.Peers()).
type LinkInfo struct {
	URI      string
	Up       bool
	Inbound  bool
	Key      string // hex public key of the remote, when the link has connected
}

// CustomPeer is one configured custom URI with its resolved key.
type CustomPeer struct {
	URI string
	Key string // hex ed25519 key; "" while unknown
	Up  bool   // outgoing link to the peer is currently up
}

// normalizePeerURI canonicalizes a peer URI for comparisons: query and
// fragment dropped, scheme and host lowercased. The core reports link URIs
// in the dialed form (query stripped), so both configured and reported URIs
// must pass through this before matching.
func normalizePeerURI(uri string) string {
	uri = strings.TrimSpace(uri)
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	u.RawQuery, u.Fragment = "", ""
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

// ResolveCustomPeers maps each configured custom URI to the peer's public
// key. The key is learned from a currently-up outgoing link (the peering
// handshake is the only way to learn a key the public list doesn't carry),
// falling back — statelessly, so fresh -once containers still probe a
// currently-down peer — to the tracker's stored rows, matched by endpoint.
func ResolveCustomPeers(uris []string, links []LinkInfo, tr *Tracker) []CustomPeer {
	live := make(map[string]CustomPeer, len(links))
	for _, l := range links {
		if !l.Up || l.Inbound || l.Key == "" {
			continue
		}
		live[normalizePeerURI(l.URI)] = CustomPeer{URI: l.URI, Key: l.Key, Up: true}
	}
	out := make([]CustomPeer, 0, len(uris))
	for _, uri := range uris {
		if c, ok := live[normalizePeerURI(uri)]; ok {
			out = append(out, CustomPeer{URI: uri, Key: c.Key, Up: true})
			continue
		}
		key := ""
		if tr != nil {
			key = tr.KeyForEndpoint(uri)
		}
		out = append(out, CustomPeer{URI: uri, Key: key})
	}
	return out
}

// MergeCustomInto adds custom peers with a known key to list (in place).
// A key already present in the list means publicnodes now carries the peer,
// and its entry wins. Returns how many of the added peers are currently up.
func MergeCustomInto(list map[string]*ListPeer, customs []CustomPeer) int {
	up := 0
	for _, c := range customs {
		if c.Key == "" {
			continue
		}
		if _, ok := list[c.Key]; ok {
			continue
		}
		ipv6, err := IPv6ForKey(c.Key)
		if err != nil {
			continue
		}
		if c.Up {
			up++
		}
		list[c.Key] = &ListPeer{
			Key:       c.Key,
			IPv6:      ipv6,
			Endpoints: []string{c.URI},
			Up:        c.Up,
		}
	}
	return up
}

// linkInfos snapshots the node's links as LinkInfo.
func (p *Poller) linkInfos() []LinkInfo {
	peers := p.node.Peers()
	out := make([]LinkInfo, 0, len(peers))
	for _, pi := range peers {
		key := ""
		if len(pi.Key) == ed25519.PublicKeySize {
			key = hex.EncodeToString(pi.Key)
		}
		out = append(out, LinkInfo{URI: pi.URI, Up: pi.Up, Inbound: pi.Inbound, Key: key})
	}
	return out
}

// ensureCustomPeerings keeps an outgoing peering to every configured custom
// peer: the handshake is how the monitor learns (and re-verifies) the peer's
// public key. Configured links redial on their own, so each URI is added
// once per process; the URIs also land in addedPeer, keeping the
// auto-selection from duplicating them.
//
// Adding alone is not enough in -once mode: the peering wait that follows
// returns as soon as ANY link is up — typically one of the five
// auto-selected ones, faster than our single custom link — and a key the
// merge cannot find means the peer is never polled by that run (and no row
// is written for the stateless fallback to pick up). So we block here,
// bounded, until every custom URI has a key from either the live link or
// the tracker.
func (p *Poller) ensureCustomPeerings(ctx context.Context) error {
	if len(p.cfg.CustomPeers) == 0 {
		return nil
	}
	for _, uri := range p.cfg.CustomPeers {
		if p.addedPeer[uri] {
			continue
		}
		if err := p.node.AddPeer(uri); err != nil {
			p.log.Warn("add custom peer", "uri", uri, "err", err)
			continue
		}
		p.addedPeer[uri] = true
		p.log.Info("custom peering added", "uri", uri)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var missing []string
		for _, uri := range p.cfg.CustomPeers {
			if !p.customKeyResolved(uri) {
				missing = append(missing, uri)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			p.log.Warn("custom peers without a key after wait", "uris", strings.Join(missing, ", "))
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// customKeyResolved reports whether uri's public key is available right now:
// from an up outgoing link, or from the tracker's stored rows.
func (p *Poller) customKeyResolved(uri string) bool {
	norm := normalizePeerURI(uri)
	for _, l := range p.linkInfos() {
		if l.Up && !l.Inbound && l.Key != "" && normalizePeerURI(l.URI) == norm {
			return true
		}
	}
	return p.tr != nil && p.tr.KeyForEndpoint(uri) != ""
}

// mergeCustomPeers rebuilds the polled list as publicnodes + custom peers
// whose key is known, refreshing listUp to match.
func (p *Poller) mergeCustomPeers() {
	if len(p.cfg.CustomPeers) == 0 {
		p.list, p.listUp = p.pubList, p.pubUp
		return
	}
	customs := ResolveCustomPeers(p.cfg.CustomPeers, p.linkInfos(), p.tr)
	merged := make(map[string]*ListPeer, len(p.pubList)+len(customs))
	for k, lp := range p.pubList {
		merged[k] = lp
	}
	addedUp := MergeCustomInto(merged, customs)
	if added := len(merged) - len(p.pubList); added > 0 {
		p.log.Info("custom peers merged", "added", added, "up", addedUp)
	}
	for _, c := range customs {
		if c.Key == "" {
			p.log.Warn("custom peer key unknown yet, not polled this cycle", "uri", c.URI)
		}
	}
	p.list, p.listUp = merged, p.pubUp+addedUp
}
