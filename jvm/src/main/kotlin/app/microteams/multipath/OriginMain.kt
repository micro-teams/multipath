/*
 * A standalone origin for the cross-language end-to-end test: a Go client dials this JVM Origin over
 * several links, opens tunnel and normal streams, and the two must interoperate byte for byte on the
 * shared wire format (redundant frames, mux, and the L5 header). Tunnels are spliced to an internal
 * echo; normal streams to an internal HTTP greeter, so one process proves both routes.
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

        val opt =
            RedundantOptions(
                n = n,
                window = 2 shl 20,
                pingIntervalMs = 20,
                deadAfterMs = 100,
                ackIntervalMs = 8,
            )
        val origin = Origin(ServerSocket(0), opt)
        val route =
            Origin.route(
                local = { Socket("127.0.0.1", app.localPort) },
                egress = { _, _ -> Socket("127.0.0.1", echo.localPort) },
            )
        println("LISTENING ${origin.port}")
        System.out.flush()
        origin.serve(route)
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

    private fun daemon(body: () -> Unit) {
        Thread(body).apply {
            isDaemon = true
            start()
        }
    }
}
