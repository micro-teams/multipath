// Generic port mapping over a MuxSession — the JVM peer of go/portmap.go. Product-agnostic "forward
// a local port to a remote service over the redundant+multiplexed transport" convenience: pure glue
// over the mux (one local connection = one mux stream), so it adds no wire format and inherits the
// mux's cross-language guarantees.
//
//   client: PortMap.forward(serverSocket, session) — accept local connections, one mux stream each.
//   server: PortMap.serve(session, handler) — accept mux streams, hand each to a handler.
//           PortMap.forwardTo(dial) is the generic handler (dial a fixed target and splice). A
//           consumer needing something else (e.g. ccproxy's MITM) supplies its own handler.
//
// Target is fixed per session/handler (ssh -L style); no per-stream address on the wire.

package app.microteams.multipath.redundant

import java.net.ServerSocket
import java.net.Socket
import kotlin.concurrent.thread

/** Handles one accepted server-side mux stream and owns closing it. */
fun interface StreamHandler {
    fun handle(stream: MuxStream)
}

object PortMap {
    /**
     * Accepts streams off a server session and dispatches each to [handler]. Blocks until the
     * session ends.
     */
    @JvmStatic
    fun serve(session: MuxSession, handler: StreamHandler) {
        while (true) {
            val st =
                try {
                    session.acceptStream()
                } catch (_: Exception) {
                    return
                }
            thread(isDaemon = true) { handler.handle(st) }
        }
    }

    /** A handler that dials a fresh target connection per stream and splices the two. */
    @JvmStatic
    fun forwardTo(dial: () -> Socket): StreamHandler = StreamHandler { st ->
        val up =
            try {
                dial()
            } catch (_: Exception) {
                st.reset()
                return@StreamHandler
            }
        spliceStreamSocket(st, up)
    }

    /** Accepts local connections on [serverSocket] and forwards each over a new mux stream. */
    @JvmStatic
    fun forward(serverSocket: ServerSocket, session: MuxSession) {
        while (true) {
            val c =
                try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    return
                }
            thread(isDaemon = true) {
                val st =
                    try {
                        session.openStream()
                    } catch (_: Exception) {
                        try {
                            c.close()
                        } catch (_: Exception) {}
                        return@thread
                    }
                spliceStreamSocket(st, c)
            }
        }
    }

    // Relays a mux stream and a socket both ways, preserving half-close: an EOF in one direction
    // half-closes the write side of the other (FIN on the stream, shutdownOutput on the socket) so
    // request/response protocols still get their reply; both ends are fully closed once both
    // directions finish.
    private fun spliceStreamSocket(st: MuxStream, c: Socket) {
        val bothDone = java.util.concurrent.CountDownLatch(2)
        // stream -> socket
        thread(isDaemon = true) {
            val out = c.getOutputStream()
            val buf = ByteArray(32 * 1024)
            try {
                while (true) {
                    val n = st.read(buf, 0, buf.size)
                    if (n < 0) break
                    out.write(buf, 0, n)
                    out.flush()
                }
            } catch (_: Exception) {}
            try {
                c.shutdownOutput()
            } catch (_: Exception) {}
            bothDone.countDown()
        }
        // socket -> stream
        thread(isDaemon = true) {
            val input = c.getInputStream()
            val buf = ByteArray(32 * 1024)
            try {
                while (true) {
                    val n = input.read(buf)
                    if (n < 0) break
                    st.write(buf.copyOfRange(0, n))
                }
            } catch (_: Exception) {}
            try {
                st.closeWrite()
            } catch (_: Exception) {}
            bothDone.countDown()
        }
        bothDone.await()
        st.reset()
        try {
            c.close()
        } catch (_: Exception) {}
    }
}
