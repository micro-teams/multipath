/*
 * L2, origin side: turn an accepted socket into a link, whatever encapsulation it arrived in.
 *
 * A link reaches the origin as raw TLS (a direct line), as a WebSocket (a CDN edge that terminates
 * TLS and forwards only HTTP), or as plaintext (a testbed, or an origin fronted by something that
 * terminated TLS already). decapLink sniffs the first byte to tell them apart — 0x16 is a TLS
 * ClientHello, 'G' is an HTTP upgrade, anything else is a raw frame stream — and unwraps to a
 * LinkConn carrying opaque bytes, which is all the redundant layer above it ever sees. Mirror of the
 * Go peer's linkListener (server.go).
 */
package app.microteams.multipath

import app.microteams.multipath.redundant.LinkConn
import java.io.Closeable
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.io.PushbackInputStream
import java.net.Socket
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket

/**
 * Decapsulates one accepted socket. [sslContext] terminates raw-TLS links; null rejects them.
 * [path] is the WebSocket upgrade path CDN-fronted links arrive on.
 */
fun decapLink(sock: Socket, sslContext: SSLContext?, path: String): LinkConn {
    val pb = PushbackInputStream(sock.getInputStream(), 1)
    val first = pb.read()
    if (first < 0) throw EOFException("multipath: empty link")
    pb.unread(first)

    return when {
        first == 0x16 -> {
            requireNotNull(sslContext) { "multipath: TLS link but no server TLS config" }
            val ssl = sslContext.socketFactory.createSocket(sock, null, true) as SSLSocket
            ssl.useClientMode = false
            ssl.startHandshake()
            sniffWsOrRaw(ssl.inputStream, ssl.outputStream, ssl, path)
        }
        first == 'G'.code -> webSocketLink(pb, sock.getOutputStream(), sock, path)
        else -> LinkConn(pb, sock.getOutputStream(), sock)
    }
}

// sniffWsOrRaw looks at a post-TLS stream: an HTTP upgrade is a WebSocket, anything else is a raw
// frame stream.
private fun sniffWsOrRaw(
    inp: InputStream,
    out: OutputStream,
    closer: Closeable,
    path: String,
): LinkConn {
    val pb = PushbackInputStream(inp, 1)
    val first = pb.read()
    if (first < 0) throw EOFException("multipath: empty link after TLS")
    pb.unread(first)
    return if (first == 'G'.code) webSocketLink(pb, out, closer, path)
    else LinkConn(pb, out, closer)
}

private fun webSocketLink(
    inp: InputStream,
    out: OutputStream,
    closer: Closeable,
    path: String,
): LinkConn {
    val ws = acceptWebSocket(inp, out, path)
    return LinkConn(
        ws.input,
        ws.output,
        Closeable {
            ws.close()
            closer.close()
        },
    )
}
