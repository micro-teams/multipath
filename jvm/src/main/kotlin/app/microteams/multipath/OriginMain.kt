/*
 * A standalone origin for the cross-language end-to-end test: a client in any of the four
 * languages dials this JVM Origin over several links and opens streams against its services, which
 * must interoperate byte-for-byte on the shared wire format (redundant frames, mux, L5 header) and,
 * for "ws-echo", the RFC 6455 handshake and framing too. "echo" is a raw byte tunnel, "greeter" a
 * one-shot HTTP reply, "ws-echo" a real WebSocket server that echoes whole messages back —
 * exercising a client's application-level WebSocket-over-the-substrate, not just a byte pipe.
 *
 * Args: <n>. Prints "LISTENING <port>" once ready, then serves.
 */
package app.microteams.multipath

import app.microteams.multipath.redundant.RedundantOptions
import java.net.ServerSocket
import java.net.Socket

object OriginMain {
    @JvmStatic
    fun main(args: Array<String>) {
        val n = args[0].toInt()

        // Internal echo target for tunnel streams.
        val echo = ServerSocket(0)
        daemon {
            while (true) {
                val c = echo.accept()
                daemon {
                    try {
                        c.getInputStream().copyTo(c.getOutputStream())
                    } catch (_: Exception) {} finally {
                        c.close()
                    }
                }
            }
        }

        // Internal HTTP greeter for normal streams: one request, one reply, then close.
        val app = ServerSocket(0)
        daemon {
            while (true) {
                val c = app.accept()
                daemon { serveOneHttp(c) }
            }
        }

        // A real WebSocket server: accepts the RFC 6455 handshake, then echoes each message back as
        // one frame of the same type. Proves an application-level WebSocket tunnels through the
        // substrate to a genuine WS backend, not just a byte pipe like "echo" above.
        val wsEcho = ServerSocket(0)
        daemon {
            while (true) {
                val c = wsEcho.accept()
                daemon { serveWsEcho(c) }
            }
        }

        val opt =
            RedundantOptions(
                n = n,
                window = 2 shl 20,
                pingIntervalMs = 20,
                deadAfterMs = 100,
                ackIntervalMs = 8,
            )
        val origin = Origin(ServerSocket(0), opt)
        val services =
            mapOf(
                "echo" to Origin.dialService("127.0.0.1", echo.localPort),
                "greeter" to Origin.dialService("127.0.0.1", app.localPort),
                "ws-echo" to Origin.dialService("127.0.0.1", wsEcho.localPort),
            )
        println("LISTENING ${origin.port}")
        System.out.flush()
        origin.serve(services)
    }

    /** Reads one HTTP request off c, replies "hello <path>", and closes. */
    private fun serveOneHttp(c: Socket) {
        try {
            val reader = c.getInputStream().bufferedReader()
            val requestLine = reader.readLine() ?: return
            val path = requestLine.split(" ").getOrElse(1) { "/" }
            while (true) {
                val line = reader.readLine() ?: break
                if (line.isEmpty()) break
            }
            val body = "hello $path"
            val response =
                "HTTP/1.1 200 OK\r\nContent-Length: ${body.toByteArray().size}\r\nConnection: close\r\n\r\n$body"
            c.getOutputStream().write(response.toByteArray())
            c.getOutputStream().flush()
        } catch (_: Exception) {} finally {
            c.close()
        }
    }

    /**
     * Accepts one RFC 6455 handshake on c, then echoes each message back as one frame — relies on
     * the (test-only) assumption that one input.read() drains exactly one frame's payload when the
     * buffer given is bigger than any message this test sends, so echoing preserves the
     * frame/message boundary.
     */
    private fun serveWsEcho(c: Socket) {
        try {
            val ws = acceptWebSocket(c.getInputStream(), c.getOutputStream(), "/echo")
            val buf = ByteArray(65536)
            while (true) {
                val n = ws.input.read(buf)
                if (n < 0) break
                ws.output.write(buf, 0, n)
                ws.output.flush()
            }
        } catch (_: Exception) {} finally {
            c.close()
        }
    }

    private fun daemon(body: () -> Unit) {
        Thread(body).apply {
            isDaemon = true
            start()
        }
    }
}
