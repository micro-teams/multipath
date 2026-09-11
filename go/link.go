// L2 — the one place that knows how to reach a line.
//
// A line is a network path to the origin; a link is one live duplex connection over it. Every link
// of a redundant stream is opened here, and nothing above this file ever repeats the choice of how
// to talk to a line. The encapsulation follows the line's transport: a direct line is a TLS socket
// straight to the origin; a CDN-fronted line — one whose edge terminates TLS and forwards only
// HTTP — is a WebSocket carried inside that HTTPS, because a raw duplex would not survive the edge.
// Plaintext variants exist for the testbed, where the "edge" is a local process on loopback and
// nothing is worth encrypting twice.
//
// There is exactly one entry point, DialLink, because there is exactly one kind of thing above it:
// a redundant stream is N links, and a link is opaque bytes. The request/response shape a consumer
// might expect is not a second way to reach a line — it is a mux stream carried over these same
// links (see mux.go), so it goes through here too.

package multipath

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"
)

// Transport is how a link to a line is encapsulated. The zero value is inferred from the line's URL
// scheme, so an ordinary registry that says nothing gets the obvious choice: https → TLS, http →
// plain TCP.
type Transport string

const (
	// TransportTLS is a raw TLS socket straight to the origin. The default for an https line.
	TransportTLS Transport = "tls"
	// TransportWSS is a WebSocket carried inside HTTPS, for a line whose edge (a CDN) terminates
	// TLS and speaks only HTTP. The default is never this — a line must ask for it, because it
	// costs a handshake round trip the direct case does not.
	TransportWSS Transport = "wss"
	// TransportTCP is a plaintext socket. Testbed only: there is no edge to hide from on loopback.
	TransportTCP Transport = "tcp"
	// TransportWS is a plaintext WebSocket. Testbed only, to exercise the WebSocket framing without
	// a certificate.
	TransportWS Transport = "ws"
)

// LinkOptions carries what DialLink cannot read off the line itself.
type LinkOptions struct {
	// TLSConfig is used for TransportTLS and TransportWSS. Nil means the platform default with the
	// origin's host as the server name.
	TLSConfig *tls.Config
	// LinkPath is the path the WebSocket upgrade is issued against on a ws/wss line. The origin
	// mounts its link acceptor here. Defaults to "/mt/link".
	LinkPath string
	// DialTimeout bounds establishing one link. Zero means no explicit timeout beyond ctx.
	DialTimeout time.Duration
	// dialer is injected in tests so a link can be opened over an in-memory pipe.
	dialer func(ctx context.Context, network, address string) (net.Conn, error)
}

const defaultLinkPath = "/mt/link"

// resolveTransport picks the encapsulation for a line: the line's explicit transport if it set one,
// otherwise the obvious default for its URL scheme.
func resolveTransport(line Line) (Transport, error) {
	switch Transport(line.Transport) {
	case TransportTLS, TransportWSS, TransportTCP, TransportWS:
		return Transport(line.Transport), nil
	case "":
		// Inferred from the scheme. A same-origin line ("") has no scheme of its own here; a link
		// needs a concrete host, so that case is the caller's to resolve before dialling.
		if line.URL == "" {
			return "", fmt.Errorf("multipath: line %q is same-origin; a link needs a concrete URL", line.ID)
		}
		u, err := url.Parse(line.URL)
		if err != nil {
			return "", fmt.Errorf("multipath: line %q has an unparseable url %q: %w", line.ID, line.URL, err)
		}
		switch u.Scheme {
		case "https":
			return TransportTLS, nil
		case "http":
			return TransportTCP, nil
		default:
			return "", fmt.Errorf("multipath: line %q url scheme %q is neither http nor https", line.ID, u.Scheme)
		}
	default:
		return "", fmt.Errorf("multipath: line %q has unknown transport %q", line.ID, line.Transport)
	}
}

// hostPort returns the host:port to dial for a line, defaulting the port to the scheme's.
func hostPort(line Line) (host, address string, err error) {
	u, err := url.Parse(line.URL)
	if err != nil {
		return "", "", fmt.Errorf("multipath: line %q has an unparseable url %q: %w", line.ID, line.URL, err)
	}
	host = u.Hostname()
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			return "", "", fmt.Errorf("multipath: line %q url has no port and scheme %q has no default", line.ID, u.Scheme)
		}
	}
	return host, net.JoinHostPort(host, port), nil
}

// DialLink opens one duplex link to a line, choosing the encapsulation the line calls for. The
// returned conn carries opaque bytes; what rides it (redundant frames, and mux streams above them)
// is decided higher up. A link is long-lived — the cost of the choice made here is paid once.
func DialLink(ctx context.Context, line Line, opt LinkOptions) (net.Conn, error) {
	transport, err := resolveTransport(line)
	if err != nil {
		return nil, err
	}
	host, address, err := hostPort(line)
	if err != nil {
		return nil, err
	}
	if opt.LinkPath == "" {
		opt.LinkPath = defaultLinkPath
	}
	if opt.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opt.DialTimeout)
		defer cancel()
	}

	raw, err := opt.dial(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("multipath: line %q dial %s: %w", line.ID, address, err)
	}

	switch transport {
	case TransportTCP:
		return raw, nil
	case TransportWS:
		return clientWebSocket(ctx, raw, host, opt.LinkPath)
	case TransportTLS:
		conn, err := tlsHandshake(ctx, raw, host, opt.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("multipath: line %q tls: %w", line.ID, err)
		}
		return conn, nil
	case TransportWSS:
		conn, err := tlsHandshake(ctx, raw, host, opt.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("multipath: line %q tls: %w", line.ID, err)
		}
		return clientWebSocket(ctx, conn, host, opt.LinkPath)
	default:
		return nil, fmt.Errorf("multipath: line %q has unhandled transport %q", line.ID, transport)
	}
}

// dial opens the raw TCP conn, honouring an injected dialer in tests.
func (opt LinkOptions) dial(ctx context.Context, address string) (net.Conn, error) {
	if opt.dialer != nil {
		return opt.dialer(ctx, "tcp", address)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", address)
}

// tlsHandshake wraps a raw conn in TLS with the origin's host as the server name and completes the
// handshake under ctx.
func tlsHandshake(ctx context.Context, raw net.Conn, host string, cfg *tls.Config) (net.Conn, error) {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" {
		clone := cfg.Clone()
		clone.ServerName = host
		cfg = clone
	}
	conn := tls.Client(raw, cfg)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}
