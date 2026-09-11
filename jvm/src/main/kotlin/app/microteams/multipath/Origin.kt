/*
 * The origin end of the substrate: accept redundant streams over every line, demultiplex each into
 * logical streams, read the one-line header off each, and splice it where it belongs.
 *
 * The whole origin is "demux, then splice". A normal stream is spliced to the origin's own service
 * on loopback — the application stays an ordinary server that never learns a line existed. A tunnel
 * stream is authorised by the consumer's policy (ticket + destination) and, if allowed, spliced to
 * its target. There is no request middleware, no idempotency, no coalescing: the redundant layer
 * already delivered each byte exactly once, so the origin sees each request exactly once.
 *
 * Mirror of the Go peer (server.go). Java is the origin; Go is the client.
 */
package app.microteams.multipath

import app.microteams.multipath.redundant.MuxSession
import app.microteams.multipath.redundant.MuxStream
import app.microteams.multipath.redundant.RedundantOptions
import app.microteams.multipath.redundant.RedundantServer
import app.microteams.multipath.redundant.asMuxTransport
import java.io.Closeable
import java.net.ServerSocket
import java.net.Socket

/**
 * Opens the upstream a stream should be spliced to, given its header, or throws to refuse it. The
 * returned socket is owned by the splice and closed with the stream.
 */
fun interface Route {
    fun open(header: Header): Socket
}

/**
 * The origin server: accept redundant streams and serve each client's streams through a [Route].
 */
class Origin(private val serverSocket: ServerSocket, private val opt: RedundantOptions) :
    Closeable {
    private val server = RedundantServer(serverSocket, opt)

    /** The bound port, useful when the server was opened on port 0. */
    val port: Int
        get() = serverSocket.localPort

    /**
     * Serves until the socket is closed. Each client and each stream gets its own daemon thread.
     */
    fun serve(route: Route) {
        while (true) {
            val rs =
                try {
                    server.accept()
                } catch (_: Exception) {
                    return
                }
            thread { serveClient(MuxSession.server(rs.asMuxTransport()), route) }
        }
    }

    private fun serveClient(session: MuxSession, route: Route) {
        while (true) {
            val st =
                try {
                    session.acceptStream()
                } catch (_: Exception) {
                    return
                }
            thread {
                val header =
                    try {
                        StreamHeader.read(st.inputStream())
                    } catch (_: Exception) {
                        st.reset()
                        return@thread
                    }
                val up =
                    try {
                        route.open(header)
                    } catch (_: Exception) {
                        st.reset()
                        return@thread
                    }
                splice(st, up)
            }
        }
    }

    override fun close() = serverSocket.close()

    companion object {
        /**
         * The standard route: a normal stream goes to [local], a tunnel to [egress]. Either may be
         * null to refuse that kind.
         */
        fun route(
            local: (() -> Socket)?,
            egress: ((target: String, ticket: ByteArray) -> Socket)?,
        ): Route = Route { header ->
            when (header.kind) {
                StreamKind.NORMAL ->
                    (local ?: throw IllegalStateException("normal streams refused")).invoke()
                StreamKind.TUNNEL ->
                    (egress ?: throw IllegalStateException("tunnels refused")).invoke(
                        header.target,
                        header.ticket,
                    )
            }
        }

        /** A [local] that dials the origin's own service at host:port each time. */
        fun dialLocal(host: String, port: Int): () -> Socket = { Socket(host, port) }
    }
}

/**
 * Relays a mux stream and a socket in both directions, preserving half-close: when one direction
 * reaches EOF it half-closes the write side of the other (FIN on the stream, shutdownOutput on the
 * socket) rather than aborting, so a request-then-EOF still gets its reply. Both ends are torn down
 * once both directions end.
 */
private fun splice(st: MuxStream, up: Socket) {
    val upToStream = thread {
        try {
            up.getInputStream().copyTo(st.outputStream())
        } catch (_: Exception) {}
        try {
            st.closeWrite()
        } catch (_: Exception) {}
    }
    try {
        st.inputStream().copyTo(up.getOutputStream())
    } catch (_: Exception) {}
    try {
        up.shutdownOutput()
    } catch (_: Exception) {}
    upToStream.join()
    try {
        st.reset()
    } catch (_: Exception) {}
    try {
        up.close()
    } catch (_: Exception) {}
}

/** Starts a daemon thread, the one-liner used throughout the origin. */
private fun thread(body: () -> Unit): Thread =
    Thread(body).apply {
        isDaemon = true
        start()
    }
