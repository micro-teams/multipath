/*
 * The origin end of the substrate: accept redundant streams over every line, demultiplex each into
 * logical streams, read the one-line header off each, and hand it to the named service it asked for.
 *
 * The whole origin is "demux, then dispatch". Every stream names a service the origin registered; the
 * origin looks the name up and hands the stream to that service's handler. A name it did not register
 * is refused with a reason. There is no "main service" and no client-chosen address, so a client can
 * only reach a service the origin put in its registry — the open-relay/SSRF surface is gone by
 * construction. A handler owns its stream and may serve it in process (no listening port anywhere) or,
 * with the dialService convenience, splice it to a real backend address; in-process is preferred,
 * because a loopback port is attack surface a named in-process service does not have.
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
 * Serves one accepted stream for a registered service. The handler owns the stream: it must read
 * and write it and eventually close or reset it. [ticket] is the opaque capability the client sent.
 */
fun interface ServiceHandler {
    fun serve(st: MuxStream, ticket: ByteArray)
}

/**
 * The origin server: accept redundant streams and dispatch each client's streams to named services.
 */
class Origin(
    private val serverSocket: ServerSocket,
    private val opt: RedundantOptions,
    // Terminates raw-TLS (direct) links; null accepts only plaintext / CDN-fronted links — enough
    // for a testbed, not for a public origin. WebSocket (CDN) and plaintext links need no config.
    sslContext: javax.net.ssl.SSLContext? = null,
    linkPath: String = "/mt/link",
) : Closeable {
    private val server = RedundantServer(serverSocket, opt) { decapLink(it, sslContext, linkPath) }

    /** The bound port, useful when the server was opened on port 0. */
    val port: Int
        get() = serverSocket.localPort

    /**
     * Serves until the socket is closed, dispatching streams to [services] by name. Each client and
     * each stream gets its own daemon thread.
     */
    fun serve(services: Map<String, ServiceHandler>) {
        while (true) {
            val rs =
                try {
                    server.accept()
                } catch (_: Exception) {
                    return
                }
            thread { serveClient(MuxSession.server(rs.asMuxTransport()), services) }
        }
    }

    private fun serveClient(session: MuxSession, services: Map<String, ServiceHandler>) {
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
                val handler = services[header.service]
                if (handler == null) {
                    // Say why: an unknown service is refused with a reason the client surfaces on
                    // read, not a bare reset indistinguishable from a network drop.
                    st.resetWithReason("unknown service: \"${header.service}\"")
                    return@thread
                }
                handler.serve(st, header.ticket)
            }
        }
    }

    override fun close() = serverSocket.close()

    companion object {
        /**
         * A [ServiceHandler] that dials host:port and splices the stream to it — the convenience
         * for a service that really is a backend address. Prefer an in-process handler where you
         * can.
         */
        fun dialService(host: String, port: Int): ServiceHandler = ServiceHandler { st, _ ->
            val up =
                try {
                    Socket(host, port)
                } catch (e: Exception) {
                    st.resetWithReason("dial $host:$port: ${e.message}")
                    return@ServiceHandler
                }
            splice(st, up)
        }
    }
}

/**
 * Relays a mux stream and a socket in both directions, preserving half-close: when one direction
 * reaches EOF it half-closes the write side of the other (FIN on the stream, shutdownOutput on the
 * socket) rather than aborting, so a request-then-EOF still gets its reply. Both ends are torn down
 * once both directions end.
 */
internal fun splice(st: MuxStream, up: Socket) {
    val fault = java.util.concurrent.atomic.AtomicBoolean(false)
    val upToStream = thread {
        try {
            up.getInputStream().copyTo(st.outputStream())
        } catch (_: Exception) {
            fault.set(true)
        }
        try {
            st.closeWrite()
        } catch (_: Exception) {}
    }
    try {
        st.inputStream().copyTo(up.getOutputStream())
    } catch (_: Exception) {
        fault.set(true)
    }
    try {
        up.shutdownOutput()
    } catch (_: Exception) {}
    upToStream.join()
    // Reset only aborts an abnormal end; a clean both-way EOF is torn down by the FINs above (the
    // mux frees the stream on both-FIN), so the peer keeps whatever it had not yet drained.
    if (fault.get()) {
        try {
            st.reset()
        } catch (_: Exception) {}
    }
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
