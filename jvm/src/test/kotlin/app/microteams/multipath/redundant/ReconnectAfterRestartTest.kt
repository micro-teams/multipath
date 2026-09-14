package app.microteams.multipath.redundant

import java.io.IOException
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.SecureRandom
import java.time.Duration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReconnectAfterRestartTest {

    private fun waitUntil(msg: String, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(5)
        }
        throw AssertionError("timed out waiting for: $msg")
    }

    private fun randomConnId(): ByteArray {
        val b = ByteArray(16)
        SecureRandom().nextBytes(b)
        return b
    }

    // An origin that has never seen a connID must refuse a HELLO2 that claims to be a reconnect of
    // it (reconnect=true) instead of silently accepting it as a new stream. This is the
    // origin-restart case: the origin's connID table is gone, but a surviving client still believes
    // its stream is the same one it always had.
    @Test
    fun reconnectOfUnknownConnIdIsRejected() {
        val srvSock = ServerSocket(0)
        val server = RedundantServer(srvSock, RedundantOptions(n = 1))

        Socket().use { sock ->
            sock.connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
            sock.getOutputStream().write(encodeHello2(randomConnId(), 0, true))
            sock.soTimeout = 2000
            val f = FrameReader(sock.getInputStream()).next()
            assertEquals(FRAME_REJECT, f.type, "want REJECT for a reconnect of an unknown connID")
            assertTrue(
                f.reason.contains("unknown connID"),
                "want a reason naming the unknown connID, got: ${f.reason}",
            )
        }

        server.close()
    }

    // The companion case: a HELLO2 that honestly claims reconnect=false for a connID the origin has
    // never seen is accepted exactly like a legacy HELLO always was.
    @Test
    fun freshConnIdStillAccepted() {
        val srvSock = ServerSocket(0)
        val server = RedundantServer(srvSock, RedundantOptions(n = 1))
        val sock = Socket()
        sock.connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
        sock.getOutputStream().write(encodeHello2(randomConnId(), 0, false))

        assertTimeoutPreemptively(Duration.ofSeconds(2)) { server.accept() }

        sock.close()
        server.close()
    }

    // The end-to-end shape of the production incident: a client's stream is fully established
    // against an origin, that origin process is replaced by a fresh one holding no memory of any
    // connID (simulated by starting a brand-new RedundantServer on the same port after closing the
    // first), and the client's reconnect must surface as an error the caller can act on — not a
    // silent, permanently wedged stream.
    @Test
    fun clientSelfHealsWhenOriginForgetsConnId() {
        val srvSock1 = ServerSocket(0)
        val port = srvSock1.localPort
        val server1 = RedundantServer(srvSock1, RedundantOptions(n = 1))
        val accepted1 = java.util.concurrent.CompletableFuture<RedundantStream>()
        Thread(
                {
                    try {
                        accepted1.complete(server1.accept())
                    } catch (_: Exception) {}
                },
                "accept-1",
            )
            .apply {
                isDaemon = true
                start()
            }

        val dial: (Int) -> LinkConn? = {
            val s = Socket("127.0.0.1", port)
            s.tcpNoDelay = true
            LinkConn(s.getInputStream(), s.getOutputStream(), s)
        }
        val client =
            RedundantStream.dial(
                RedundantOptions(
                    n = 1,
                    pingIntervalMs = 5,
                    deadAfterMs = 500,
                    reconnectDelayMs = 5,
                    maxDelayMs = 20,
                ),
                dial,
            )

        waitUntil("the first link established against the original origin") {
            client.stats()[0].state == "up"
        }
        // "up" fires the instant the TCP dial + HELLO write succeed — before the origin's very
        // first reply has necessarily been read. `established` only flips on that reply, so wait
        // for it explicitly via a PING/PONG round trip rather than assuming "up" already implies
        // it.
        Thread.sleep(200)

        // "Restart" the origin: close the old server socket and the stream it accepted (closing the
        // ServerSocket alone does not touch sockets it already accepted — a real process exit
        // would, this can't, so the established stream is closed by hand right after), then stand
        // up a brand-new RedundantServer on the same port, holding no memory of this client's
        // connID.
        srvSock1.close()
        accepted1.get(2, java.util.concurrent.TimeUnit.SECONDS).close()
        val srvSock2 = ServerSocket()
        srvSock2.reuseAddress = true
        srvSock2.bind(InetSocketAddress("127.0.0.1", port))
        val server2 = RedundantServer(srvSock2, RedundantOptions(n = 1))
        Thread(
                {
                    try {
                        server2.accept()
                    } catch (_: Exception) {}
                },
                "accept-2",
            )
            .apply {
                isDaemon = true
                start()
            }

        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            val ex = assertThrows(IOException::class.java) { client.read(ByteArray(16), 0, 16) }
            assertTrue(
                ex.message!!.contains("rejected"),
                "want a rejected-by-origin error once the origin no longer recognized its connID, " +
                    "got: ${ex.message}",
            )
        }

        client.close()
        server2.close()
    }
}
