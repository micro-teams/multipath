// The PROXY protocol version 2 header (haproxy.org/download/2.9/doc/proxy-protocol.txt) that tells the
// target which client a spliced connection belongs to. Without it every request reaches the target
// from the server's own address. nginx reads it with `listen ... proxy_protocol`.
//
// The source is the client address of the session (PROTOCOL.md section 10) with port 0, since a
// forwarding header carries no port; the destination is the address the client's link reached.
// A source that is not an IP address (it cannot be, unless a front proxy sent garbage) is sent as
// the LOCAL command, which tells the target to use the connection's own addresses.

package server

import (
	"encoding/binary"
	"net"
)

var proxySignature = []byte("\r\n\r\n\x00\r\nQUIT\n")

// proxyHeader builds the header for a client address and the local address of its link.
func proxyHeader(client string, local net.Addr) []byte {
	b := append([]byte(nil), proxySignature...)
	src := net.ParseIP(client)
	var dst net.IP
	var dstPort int
	if tcp, ok := local.(*net.TCPAddr); ok {
		dst, dstPort = tcp.IP, tcp.Port
	}
	if src == nil || dst == nil {
		return append(b, 0x20, 0x00, 0x00, 0x00) // LOCAL, unspecified family, no addresses
	}
	if src4, dst4 := src.To4(), dst.To4(); src4 != nil && dst4 != nil {
		b = append(b, 0x21, 0x11, 0x00, 12) // PROXY, TCP over IPv4, 12 bytes of addresses
		b = append(append(b, src4...), dst4...)
	} else {
		b = append(b, 0x21, 0x21, 0x00, 36) // PROXY, TCP over IPv6, 36 bytes of addresses
		b = append(append(b, src.To16()...), dst.To16()...)
	}
	b = binary.BigEndian.AppendUint16(b, 0)
	return binary.BigEndian.AppendUint16(b, uint16(dstPort))
}
