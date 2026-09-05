package app.microteams.multipath.redundant

import java.net.ServerSocket
import java.net.Socket
import java.util.Random
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RedundantTest {

    private fun hex(b: ByteArray) = b.joinToString("") { "%02x".format(it) }

    // ---- cross-language wire compatibility (golden bytes emitted by the Go encoder) ----

    @Test
    fun encoderMatchesGoWire() {
        val id = ByteArray(16) { (it + 1).toByte() }
        assertEquals("050102030405060708090a0b0c0d0e0f100003", hex(encodeHello(id, 3)))
        assertEquals(
            "01000000000000002a00053610a68668656c6c6f",
            hex(encodeData(42, "hello".toByteArray(), 0, 5)),
        )
        assertEquals("020000000000003039", hex(encodeAck(12345)))
        assertEquals("030000000000000007", hex(encodeNonce(FRAME_PING, 7)))
    }

    @Test
    fun decoderReadsGoWire() {
        val bytes = hexToBytes("01000000000000002a00053610a68668656c6c6f")
        val f = FrameReader(bytes.inputStream()).next()
        assertEquals(FRAME_DATA, f.type)
        assertEquals(42L, f.offset)
        assertArrayEquals("hello".toByteArray(), f.payload)
    }

    @Test
    fun decoderRejectsBadCrc() {
        val bytes = hexToBytes("01000000000000002a00053610a68668656c6c6f")
        bytes[bytes.size - 1] = (bytes[bytes.size - 1].toInt() xor 0xFF).toByte()
        var threw = false
        try {
            FrameReader(bytes.inputStream()).next()
        } catch (_: CorruptFrameException) {
            threw = true
        }
        assertTrue(threw, "expected CorruptFrameException on bad CRC")
    }

    private fun hexToBytes(s: String): ByteArray =
        ByteArray(s.length / 2) {
            ((s[it * 2].digitToInt(16) shl 4) or s[it * 2 + 1].digitToInt(16)).toByte()
        }

    // ---- real-TCP end-to-end, JVM client + JVM server, fault middlebox ----

    @Test
    fun endToEndOverTcpUnderFaults() {
        val n = 4
        val opt =
            RedundantOptions(
                n = n,
                window = 2 shl 20,
                pingIntervalMs = 20,
                deadAfterMs = 80,
                ackIntervalMs = 8,
                reconnectDelayMs = 5,
                maxDelayMs = 40,
            )
        val srvSock = ServerSocket(0)
        val server = RedundantServer(srvSock, opt)
        val boxes = (0 until n).map { Middlebox(srvSock.localPort, seed = 1000L + it) }

        val client =
            RedundantStream.dial(opt) { i ->
                val s = Socket("127.0.0.1", boxes[i].port)
                s.tcpNoDelay = true
                LinkConn(s.getInputStream(), s.getOutputStream(), s)
            }

        val accepted = server.accept()

        val msg = ByteArray(256 * 1024).also { Random(7).nextBytes(it) }
        thread(isDaemon = true) { client.write(msg) }

        val got = ByteArray(msg.size)
        var have = 0
        val deadline = System.currentTimeMillis() + 30_000
        while (have < msg.size) {
            assertTrue(
                System.currentTimeMillis() < deadline,
                "timeout at $have/${msg.size} under faults",
            )
            val m = accepted.read(got, have, msg.size - have)
            assertTrue(m >= 0, "unexpected EOF at $have")
            have += m
        }
        assertArrayEquals(msg, got, "payload mismatch under fault injection")

        client.close()
        accepted.close()
        server.close()
        boxes.forEach { it.close() }
    }
}

/**
 * A per-link fault-injecting TCP middlebox: black-hole (connection stays open, bytes silently
 * dropped), one-directional cut, and hard disconnect, on a random schedule. Mirrors the Go
 * middlebox.
 */
private class Middlebox(target: Int, seed: Long) {
    private val ln = ServerSocket(0)
    private val rng = Random(seed)
    val port: Int = ln.localPort

    init {
        thread(isDaemon = true) {
            while (true) {
                val client =
                    try {
                        ln.accept()
                    } catch (_: Exception) {
                        return@thread
                    }
                thread(isDaemon = true) { serve(client, target) }
            }
        }
    }

    private fun serve(client: Socket, target: Int) {
        val server =
            try {
                Socket("127.0.0.1", target)
            } catch (_: Exception) {
                client.close()
                return
            }
        val dropCS = AtomicBoolean(false)
        val dropSC = AtomicBoolean(false)
        val closed = AtomicBoolean(false)
        val stop = {
            if (!closed.getAndSet(true)) {
                try {
                    client.close()
                } catch (_: Exception) {}
                try {
                    server.close()
                } catch (_: Exception) {}
            }
        }
        thread(isDaemon = true) { govern(dropCS, dropSC, closed, stop) }
        val t1 =
            thread(isDaemon = true) {
                pump(client, server, dropCS, closed)
                stop()
            }
        pump(server, client, dropSC, closed)
        stop()
        t1.join()
    }

    private fun pump(src: Socket, dst: Socket, drop: AtomicBoolean, closed: AtomicBoolean) {
        val buf = ByteArray(32 * 1024)
        src.soTimeout = 20
        while (!closed.get()) {
            val n =
                try {
                    src.getInputStream().read(buf)
                } catch (e: java.net.SocketTimeoutException) {
                    continue
                } catch (e: Exception) {
                    return
                }
            if (n < 0) return
            if (!drop.get()) {
                try {
                    dst.getOutputStream().write(buf, 0, n)
                    dst.getOutputStream().flush()
                } catch (_: Exception) {
                    return
                }
            }
        }
    }

    private fun govern(
        dropCS: AtomicBoolean,
        dropSC: AtomicBoolean,
        closed: AtomicBoolean,
        stop: () -> Unit,
    ) {
        while (!closed.get()) {
            Thread.sleep((3 + rng.nextInt(13)).toLong())
            when (rng.nextInt(5)) {
                0,
                1 -> { // black-hole both directions briefly
                    dropCS.set(true)
                    dropSC.set(true)
                    Thread.sleep((3 + rng.nextInt(16)).toLong())
                    dropCS.set(false)
                    dropSC.set(false)
                }
                2 -> {
                    dropCS.set(true)
                    Thread.sleep((3 + rng.nextInt(16)).toLong())
                    dropCS.set(false)
                }
                3 -> {
                    dropSC.set(true)
                    Thread.sleep((3 + rng.nextInt(16)).toLong())
                    dropSC.set(false)
                }
                4 -> {
                    stop()
                    return
                }
            }
        }
    }

    fun close() {
        try {
            ln.close()
        } catch (_: Exception) {}
    }
}
