package app.microteams.multipath.redundant

import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.Collections
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RejectTest {

    private fun waitUntil(msg: String, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(5)
        }
        throw AssertionError("timed out waiting for: $msg")
    }

    private fun dialTo(port: Int): (Int) -> LinkConn? = {
        val s = Socket("127.0.0.1", port)
        s.tcpNoDelay = true
        LinkConn(s.getInputStream(), s.getOutputStream(), s)
    }

    // A link whose HELLO index the origin refuses is reported down with the origin's reason and
    // never
    // retried, while an in-range link on the same stream stays up.
    @Test
    fun outOfRangeLinkReportedNotRetried() {
        val events = Collections.synchronizedList(mutableListOf<LinkState>())
        val srvSock = ServerSocket(0)
        val server = RedundantServer(srvSock, RedundantOptions(n = 1)) // accepts index 0 only
        val client =
            RedundantStream.dial(
                RedundantOptions(
                    n = 2, // offers index 0 and 1; 1 is out of range
                    pingIntervalMs = 20,
                    deadAfterMs = 500,
                    reconnectDelayMs = 10,
                    maxDelayMs = 50,
                    onLinkState = { events.add(it) },
                ),
                dialTo(srvSock.localPort),
            )
        val accepted = server.accept()

        waitUntil("link 1 reported down with the out-of-range reason") {
            events.any { it.index == 1 && !it.up && it.reason.contains("out of range") }
        }
        waitUntil("link 0 up") { client.stats()[0].state == "up" }
        Thread.sleep(200)
        assertEquals(0, client.stats()[1].reconnects, "rejected link 1 must not be retried")

        client.close()
        accepted.close()
        server.close()
    }

    // The origin refuses every link: dial still returns (HELLO was written), but the stream then
    // closes with the reason so read fails fast instead of hanging forever.
    @Test
    fun allLinksRejectedFailFast() {
        val srvSock = ServerSocket(0)
        val server =
            RedundantServer(srvSock, RedundantOptions(n = 0)) // accepts no index → rejects all
        val client =
            RedundantStream.dial(
                RedundantOptions(n = 1, pingIntervalMs = 20, reconnectDelayMs = 10),
                dialTo(srvSock.localPort),
            )

        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            val ex = assertThrows(IOException::class.java) { client.read(ByteArray(16), 0, 16) }
            assertTrue(
                ex.message!!.contains("rejected"),
                "read should fail with the reject reason, got: ${ex.message}",
            )
        }

        client.close()
        server.close()
    }
}
