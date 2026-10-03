// Package ygg wraps the yggdrasil-go core as an embedded, headless library
// node: no TUN device, no admin socket. The node peers with a few public
// peers and exposes the global tree state via GetTree/GetPaths.
package ygg

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Arceliar/ironwood/types"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// Node is an embedded Yggdrasil core node.
type Node struct {
	core     *core.Core
	priv     ed25519.PrivateKey
	DataDir  string
	PeerURIs []string
}

// LoadOrGenerateKey reads <dir>/node.key (hex ed25519 private key) or
// creates a new one. The identity is stable across restarts.
func LoadOrGenerateKey(dir string) (ed25519.PrivateKey, error) {
	path := filepath.Join(dir, "node.key")
	if b, err := os.ReadFile(path); err == nil {
		raw, err := hex.DecodeString(strings.TrimSpace(string(b)))
		if err != nil || len(raw) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("ygg: %s is not a valid ed25519 private key", path)
		}
		return ed25519.PrivateKey(raw), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(priv)), 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}

// selfSignedCert mirrors yggdrasil's own certificate template: a self-signed
// ed25519 certificate with the node key as CommonName, used only for the
// peering TLS handshake.
func selfSignedCert(priv ed25519.PrivateKey) (*tls.Certificate, error) {
	pub := priv.Public().(ed25519.PublicKey)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: hex.EncodeToString(pub)},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(100, 0, 0),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, nil
}

// New starts an embedded node peering with the given URIs (tcp://, tls://,
// quic://, ws://...). dir holds the persistent identity key.
func New(dir string, peers []string, logger core.Logger) (*Node, error) {
	priv, err := LoadOrGenerateKey(dir)
	if err != nil {
		return nil, fmt.Errorf("ygg: load key: %w", err)
	}
	cert, err := selfSignedCert(priv)
	if err != nil {
		return nil, fmt.Errorf("ygg: cert: %w", err)
	}
	opts := make([]core.SetupOption, 0, len(peers))
	for _, p := range peers {
		opts = append(opts, core.Peer{URI: p})
	}
	c, err := core.New(cert, logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("ygg: core: %w", err)
	}
	return &Node{core: c, priv: priv, DataDir: dir, PeerURIs: peers}, nil
}

// PublicKey returns the node's own public key in hex.
func (n *Node) PublicKey() string {
	return hex.EncodeToString(n.priv.Public().(ed25519.PublicKey))
}

func (n *Node) Stop() {
	n.core.Stop()
}

func (n *Node) Tree() []core.TreeEntryInfo  { return n.core.GetTree() }
func (n *Node) Paths() []core.PathEntryInfo { return n.core.GetPaths() }
func (n *Node) Peers() []core.PeerInfo      { return n.core.GetPeers() }

// PathsMap returns currently cached coordinates for every node the monitor
// has learned a path to, keyed by hex public key, coords formatted "1.2.3".
// (Local twin of collector.PathString — kept here to avoid an import cycle.)
func (n *Node) PathsMap() map[string]string {
	paths := n.core.GetPaths()
	m := make(map[string]string, len(paths))
	for _, p := range paths {
		parts := make([]string, len(p.Path))
		for i, v := range p.Path {
			parts[i] = strconv.FormatUint(v, 10)
		}
		m[hex.EncodeToString(p.Key)] = strings.Join(parts, ".")
	}
	return m
}

// PeersUp returns the number of currently connected outgoing peerings.
func (n *Node) PeersUp() int {
	nUp := 0
	for _, p := range n.core.GetPeers() {
		if p.Up && !p.Inbound {
			nUp++
		}
	}
	return nUp
}

// AddPeer adds an outgoing peering at runtime.
func (n *Node) AddPeer(uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		return err
	}
	return n.core.AddPeer(u, "")
}

// Probe sends one harmless packet to the node with the given public key.
// If no path (coordinates) to that node is cached, ironwood floods a lookup
// request toward it; the destination itself answers with a signed path
// notification. Dead nodes never answer, which is exactly the liveness
// signal we record. The payload is delivered as garbage IPv6 and dropped by
// the remote kernel — path learning happens below the IP layer.
func (n *Node) Probe(key ed25519.PublicKey) error {
	_, err := n.core.WriteTo([]byte{0}, types.Addr(key))
	return err
}

// SetPathNotify registers a callback fired when a path to a node is learned
// or refreshed (i.e. when a lookup gets answered).
func (n *Node) SetPathNotify(f func(ed25519.PublicKey)) { n.core.SetPathNotify(f) }
