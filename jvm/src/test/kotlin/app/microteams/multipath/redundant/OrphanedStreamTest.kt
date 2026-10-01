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
import org.junit.jupiter.api.Test

class OrphanedStreamTest {

    private fun randomConnId(): ByteArray = ByteArray(16).also { SecureRandom().nextBytes(it) }

    // A client that goes away for good — its machine was destroyed, or it redialled under a new
    // connID — never sends anything again. The origin's side of its stream must end on its own:
    // left open, each one keeps its keepalive, ack and mux threads forever, and an origin that
    // serves machines which come and go runs out of memory.
    @Test
    fun serverStreamWithNoLinkLeftEnds() {
        val srvSock = ServerSocket(0)
        val opt =
            RedundantOptions(n = 1, pingIntervalMs = 20, deadAfterMs = 100, orphanAfterMs = 300)
        val server = RedundantServer(srvSock, opt)
        val connId = randomConnId()

        val sock = Socket()
        sock.connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
        sock.getOutputStream().write(encodeHello2(connId, 0, false))
        val stream: RedundantStream = server.accept()
        sock.close()

        assertTimeoutPreemptively(Duration.ofSeconds(3)) {
            assertThrows(IOException::class.java) { stream.inputStream().read() }
        }

        // The origin also forgot the connID, so a late return is told to start over rather than
        // re-attaching under a stream nobody serves any more.
        Socket().use { late ->
            late.connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
            late.getOutputStream().write(encodeHello2(connId, 0, true))
            late.soTimeout = 2000
            assertEquals(FRAME_REJECT, FrameReader(late.getInputStream()).next().type)
        }
        server.close()
    }

    // A link that drops and comes back within the window keeps its stream: the orphan timer is
    // for clients that are gone, not for ones that reconnect.
    @Test
    fun linkThatReturnsInTimeKeepsTheStream() {
        val srvSock = ServerSocket(0)
        val opt =
            RedundantOptions(n = 1, pingIntervalMs = 20, deadAfterMs = 100, orphanAfterMs = 1000)
        val server = RedundantServer(srvSock, opt)
        val connId = randomConnId()

        Socket()
            .apply {
                connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
                getOutputStream().write(encodeHello2(connId, 0, false))
            }
            .also { first ->
                assertTimeoutPreemptively(Duration.ofSeconds(2)) { server.accept() }
                first.close()
            }
        Thread.sleep(300)

        Socket().use { back ->
            back.connect(InetSocketAddress("127.0.0.1", srvSock.localPort))
            back.getOutputStream().write(encodeHello2(connId, 0, true))
            back.soTimeout = 500
            val f = FrameReader(back.getInputStream()).next()
            // Re-attached: the origin talks to it (PING/ACK) instead of refusing it.
            assert(f.type != FRAME_REJECT) { "a link back within the window was rejected" }
        }
        server.close()
    }
}
