// Package multipath aggregates several redundant network paths to one origin into a single reliable
// transport, and carries everything — application requests, WebSockets, opaque tunnels — over it as
// multiplexed streams.
//
// One idea runs through the whole package: send on every path at once, and de-duplicate at the
// receiver. The redundant stream writes every byte to every live link and delivers each byte once,
// from whichever link arrived first; a dead link is simply never the fastest, so there is no line to
// choose, no timeout to wait out, and no request to retry. Above that, a mux carries many logical
// streams over the one redundant stream, and a one-line header on each says what it is: a normal
// exchange with the origin's own service, or a tunnel to some target.
//
// The layers, bottom to top:
//
//	registry.go   the lines: a shared JSON document naming the paths to the origin
//	link.go       L2 — one duplex link per line (TLS direct, WebSocket over a CDN, or plaintext)
//	redundant.go  L3 — N links into one reliable, ordered, never-interrupted byte stream
//	mux.go        L4 — many logical streams over the one redundant stream
//	header.go     L5 — kind, target, and an opaque egress ticket, once per stream
//	client.go     the client: Dial once, then OpenTunnel / OpenNormal / RoundTrip
//	server.go     the origin: accept, demux, and splice each stream where its header says
//
// The origin authorises nothing itself: a normal stream is spliced to the local application, which
// never learns a line existed, and a tunnel's ticket and destination are the consumer's policy, not
// this library's. The registry JSON is the one contract shared with the peer implementations.
package multipath
