package app.microteams.multipath.redundant

import java.net.ServerSocket
import java.net.Socket
import java.util.Collections
import java.util.concurrent.atomic.AtomicReferenceArray
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ObserveTest {

    private fun waitUntil(msg: String, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(5)
        }
        throw AssertionError("timed out waiting for: $msg")
    }

    // Both links come up, dropping one is reported down with a reason while the stream survives on
    // the
    // other, and the dropped link reconnects back to up.
    @Test
    fun linkStateAndStatsAreObservable() {
        val events = Collections.synchronizedList(mutableListOf<LinkState>())
        val base =
            RedundantOptions(
                n = 2,
                pingIntervalMs = 20,
                deadAfterMs = 200,
                ackIntervalMs = 8,
                reconnectDelayMs = 5,
                maxDelayMs = 40,
            )
        val srvSock = ServerSocket(0)
        val server = RedundantServer(srvSock, base)

        val socks = AtomicReferenceArray<Socket?>(2)
        val client =
            RedundantStream.dial(base.copy(onLinkState = { events.add(it) })) { i ->
                val s = Socket("127.0.0.1", srvSock.localPort)
                s.tcpNoDelay = true
                socks.set(i, s)
                LinkConn(s.getInputStream(), s.getOutputStream(), s)
            }
        val accepted = server.accept()

        waitUntil("both links up") { client.stats().all { it.state == "up" } }
        assertTrue(events.any { it.up && it.index == 0 } && events.any { it.up && it.index == 1 })

        // Drop link 0 under the stream's feet.
        socks.get(0)?.close()
        waitUntil("link 0 reported down with a reason") {
            events.any { !it.up && it.index == 0 && it.reason.isNotEmpty() }
        }
        assertTrue(client.stats()[1].state == "up", "survivor link 1 must stay up")

        // It reconnects to the still-listening server and returns to up.
        waitUntil("link 0 recovered") {
            client.stats()[0].let { it.state == "up" && it.reconnects >= 1 }
        }

        client.close()
        accepted.close()
        server.close()
    }
}
