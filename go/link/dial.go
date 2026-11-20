// The client side of a link: reach a line's ws:// or wss:// URL, through an HTTP proxy when one is
// configured, and perform the WebSocket handshake. A machine that can reach the internet only through
// a proxy must still connect, so the proxy decision is made here, for every link, the same way
// net/http makes it for a request.
//
//   - Options carries the TLS configuration for wss lines and the proxy function. Proxy has the
//     signature of http.Transport.Proxy, so http.ProxyFromEnvironment (HTTPS_PROXY, HTTP_PROXY,
//     NO_PROXY) and http.ProxyURL(u) both fit; nil means a direct connection.
//
//   - Dial(ctx, rawURL, opt) returns a client-side *Conn for "ws://host[:port]/path" or
//     "wss://host[:port]/path". ctx bounds the whole setup, proxy and handshake included. An http://
//     proxy is used with CONNECT, with Basic credentials taken from the proxy URL; other proxy
//     schemes are refused with an error rather than bypassed.
//
//     c, err := link.Dial(ctx, "wss://cf.example.app/multipath/v3/link",
//     link.Options{Proxy: http.ProxyFromEnvironment})
package link

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options configures Dial.
type Options struct {
	TLSConfig *tls.Config                           // for wss; nil means the system roots and the URL's host
	Proxy     func(*http.Request) (*url.URL, error) // nil means connect directly
}

// Dial opens one link to a ws:// or wss:// URL.
func Dial(ctx context.Context, rawURL string, opt Options) (*Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	secure := u.Scheme == "wss"
	if !secure && u.Scheme != "ws" {
		return nil, fmt.Errorf("multipath: link url %q is not ws or wss", rawURL)
	}
	port := u.Port()
	if port == "" {
		port = map[bool]string{true: "443", false: "80"}[secure]
	}
	address := net.JoinHostPort(u.Hostname(), port)

	raw, err := dialTCP(ctx, u, address, opt.Proxy)
	if err != nil {
		return nil, err
	}
	// Every step below honours ctx through the socket deadline, so a silent line cannot hold a
	// dial longer than the caller allowed.
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline) // a socket that refuses a deadline fails its next read anyway
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Unix(1, 0)) }) // unblocks the handshake
	defer stop()

	if secure {
		cfg := &tls.Config{}
		if opt.TLSConfig != nil {
			cfg = opt.TLSConfig.Clone()
		}
		if cfg.ServerName == "" {
			cfg.ServerName = u.Hostname()
		}
		conn := tls.Client(raw, cfg)
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close() // the handshake error is what the caller needs
			return nil, fmt.Errorf("multipath: tls to %s: %w", address, err)
		}
		raw = conn
	}
	c, err := clientHandshake(raw, u)
	if err != nil {
		_ = raw.Close() // the handshake error is what the caller needs
		return nil, err
	}
	if err := raw.SetDeadline(time.Time{}); err != nil {
		_ = raw.Close() // the deadline error is what the caller needs
		return nil, err
	}
	return c, nil
}

// dialTCP connects to address, directly or through the proxy the proxy function picks for u.
func dialTCP(ctx context.Context, u *url.URL, address string, proxy func(*http.Request) (*url.URL, error)) (net.Conn, error) {
	var d net.Dialer
	var proxyURL *url.URL
	if proxy != nil {
		// Proxy functions decide by the http(s) scheme, as for a request to the same host.
		probe := &http.Request{URL: &url.URL{Scheme: strings.Replace(u.Scheme, "ws", "http", 1), Host: u.Host}}
		var err error
		if proxyURL, err = proxy(probe); err != nil {
			return nil, fmt.Errorf("multipath: proxy for %s: %w", u.Host, err)
		}
	}
	if proxyURL == nil {
		c, err := d.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, fmt.Errorf("multipath: dial %s: %w", address, err)
		}
		return c, nil
	}
	if proxyURL.Scheme != "http" {
		return nil, fmt.Errorf("multipath: proxy %s: only http:// proxies are supported", proxyURL.Redacted())
	}
	proxyAddr := proxyURL.Host
	if proxyURL.Port() == "" {
		proxyAddr = net.JoinHostPort(proxyURL.Hostname(), "80")
	}
	c, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("multipath: dial proxy %s: %w", proxyURL.Redacted(), err)
	}
	if err := connect(ctx, c, proxyURL, address); err != nil {
		_ = c.Close() // the CONNECT error is what the caller needs
		return nil, fmt.Errorf("multipath: proxy %s: %w", proxyURL.Redacted(), err)
	}
	return c, nil
}

// connect asks an HTTP proxy for a tunnel to address on c.
func connect(ctx context.Context, c net.Conn, proxyURL *url.URL, address string) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(deadline) // a socket that refuses a deadline fails its next read anyway
	}
	req := "CONNECT " + address + " HTTP/1.1\r\nHost: " + address + "\r\n"
	if user := proxyURL.User; user != nil {
		password, _ := user.Password()
		req += "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(user.Username()+":"+password)) + "\r\n"
	}
	if _, err := io.WriteString(c, req+"\r\n"); err != nil {
		return err
	}
	// The proxy says nothing after its response until the tunnel carries bytes, so reading the
	// response through a buffered reader cannot swallow tunnel bytes.
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodConnect})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("CONNECT %s: %s", address, resp.Status)
	}
	return nil
}

// clientHandshake sends the upgrade request for u on raw and checks the answer.
func clientHandshake(raw net.Conn, u *url.URL) (*Conn, error) {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	path := u.RequestURI()
	request := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(raw, request); err != nil {
		return nil, err
	}
	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("multipath: websocket handshake with %s: %w", u.Host, err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("multipath: websocket handshake with %s%s: %s", u.Host, path, resp.Status)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") || resp.Header.Get("Sec-WebSocket-Accept") != acceptToken(key) {
		return nil, fmt.Errorf("multipath: websocket handshake with %s did not confirm the upgrade", u.Host)
	}
	return &Conn{raw: raw, br: br, client: true}, nil
}
