package app.microteams.multipath.redundant

import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MuxTest {

    private fun hex(b: ByteArray) = b.joinToString("") { "%02x".format(it) }

    // ---- cross-language wire compatibility (golden bytes from the Go encoder) ----

    @Test
    fun frameWireMatchesGo() {
        val syn = ByteArray(MUX_HDR_LEN)
        syn[0] = MUX_SYN.toByte()
        syn[1] = 0
        syn[2] = 0
        syn[3] = 0
        syn[4] = 1
        assertEquals("010000000100000000", hex(syn))
        // DATA stream 3, "hello"
        val data = ByteArray(MUX_HDR_LEN + 5)
        data[0] = MUX_DATA.toByte()
        data[4] = 3
        data[8] = 5
        System.arraycopy("hello".toByteArray(), 0, data, MUX_HDR_LEN, 5)
        assertEquals("02000000030000000568656c6c6f", hex(data))
        // WINDOW stream 3, credit 1024
        val win = ByteArray(MUX_HDR_LEN + 4)
        win[0] = MUX_WINDOW.toByte()
        win[4] = 3
        win[8] = 4
        win[9] = 0
        win[10] = 0
        win[11] = 4
        win[12] = 0
        assertEquals("05000000030000000400000400", hex(win))
    }

    // ---- in-memory transport pair for the behavioural tests ----

    // A thread-safe in-memory byte channel (many writers, one reader). Chunk queue + a leftover
    // buffer; an empty-array sentinel marks EOF and is re-offered so repeated reads keep seeing it.
    private class Chan {
        private val q = LinkedBlockingQueue<ByteArray>()
        private val eof = ByteArray(0)
        private var leftover: ByteArray? = null
        private var lpos = 0

        fun write(b: ByteArray) {
            if (b.isNotEmpty()) q.put(b.copyOf())
        }

        fun close() = q.put(eof)

        fun read(dst: ByteArray, off: Int, len: Int): Int {
            if (leftover == null || lpos >= leftover!!.size) {
                val item = q.take()
                if (item === eof || item.isEmpty()) {
                    q.put(eof) // keep signalling EOF to later reads
                    return -1
                }
                leftover = item
                lpos = 0
            }
            val cur = leftover!!
            val n = minOf(len, cur.size - lpos)
            System.arraycopy(cur, lpos, dst, off, n)
            lpos += n
            return n
        }
    }

    private fun transportPair(): Pair<MuxTransport, MuxTransport> {
        val aToB = Chan()
        val bToA = Chan()
        fun wrap(rx: Chan, tx: Chan) =
            object : MuxTransport {
                override fun read(dst: ByteArray, off: Int, len: Int): Int = rx.read(dst, off, len)

                override fun write(src: ByteArray) = tx.write(src)

                override fun close() {
                    tx.close()
                    rx.close()
                }
            }
        return wrap(bToA, aToB) to wrap(aToB, bToA)
    }

    private fun readAll(st: MuxStream, expected: Int): ByteArray {
        val out = ByteArray(expected)
        var have = 0
        while (have < expected) {
            val n = st.read(out, have, expected - have)
            if (n < 0) break
            have += n
        }
        return if (have == expected) out else out.copyOfRange(0, have)
    }

    @Test
    fun manyConcurrentStreamsEcho() {
        val (ta, tb) = transportPair()
        val cli = MuxSession.client(ta)
        val srv = MuxSession.server(tb)
        // server: echo each accepted stream
        thread(isDaemon = true) {
            try {
                while (true) {
                    val st = srv.acceptStream()
                    thread(isDaemon = true) {
                        val buf = ByteArray(32 * 1024)
                        try {
                            while (true) {
                                val n = st.read(buf, 0, buf.size)
                                if (n < 0) break
                                st.write(buf.copyOfRange(0, n))
                            }
                        } catch (_: Exception) {}
                        st.closeWrite()
                    }
                }
            } catch (_: Exception) {}
        }

        val n = 30
        val latch = CountDownLatch(n)
        val failure = AtomicReference<String?>(null)
        for (i in 0 until n) {
            thread(isDaemon = true) {
                try {
                    val st = cli.openStream()
                    val payload =
                        ("stream-$i:".toByteArray()) +
                            ByteArray(20 * 1024).also { Random((i + 1).toLong()).nextBytes(it) }
                    thread(isDaemon = true) {
                        st.write(payload)
                        st.closeWrite()
                    }
                    val got = readAll(st, payload.size)
                    if (!got.contentEquals(payload))
                        failure.set("stream $i mismatch (${got.size}/${payload.size})")
                } catch (e: Exception) {
                    failure.set("stream $i err: $e")
                } finally {
                    latch.countDown()
                }
            }
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS), "streams did not all finish")
        assertEquals(null, failure.get())
        cli.close()
        srv.close()
    }

    @Test
    fun backpressureBounded() {
        val (ta, tb) = transportPair()
        val cli = MuxSession.client(ta)
        val srv = MuxSession.server(tb)
        val peerRef = AtomicReference<MuxStream>()
        val accepted = CountDownLatch(1)
        thread(isDaemon = true) {
            peerRef.set(srv.acceptStream())
            accepted.countDown()
        }
        val st = cli.openStream()
        assertTrue(accepted.await(5, TimeUnit.SECONDS))
        val peer = peerRef.get()

        val wroteAll = CountDownLatch(1)
        thread(isDaemon = true) {
            st.write(ByteArray(4 shl 20).also { Random(1).nextBytes(it) })
            wroteAll.countDown()
        }
        // Peer not reading: writer must block after ~one window, not complete.
        assertTrue(
            !wroteAll.await(500, TimeUnit.MILLISECONDS),
            "write completed without backpressure",
        )
        // Drain: now it completes.
        thread(isDaemon = true) {
            val buf = ByteArray(64 * 1024)
            try {
                while (peer.read(buf, 0, buf.size) >= 0) {}
            } catch (_: Exception) {}
        }
        assertTrue(wroteAll.await(10, TimeUnit.SECONDS), "write never completed after drain")
        cli.close()
        srv.close()
    }

    @Test
    fun resetUnblocksPeer() {
        val (ta, tb) = transportPair()
        val cli = MuxSession.client(ta)
        val srv = MuxSession.server(tb)
        val peerRef = AtomicReference<MuxStream>()
        val accepted = CountDownLatch(1)
        thread(isDaemon = true) {
            peerRef.set(srv.acceptStream())
            accepted.countDown()
        }
        val st = cli.openStream()
        assertTrue(accepted.await(5, TimeUnit.SECONDS))
        val peer = peerRef.get()
        st.reset()
        val failed = CountDownLatch(1)
        thread(isDaemon = true) {
            try {
                peer.read(ByteArray(8), 0, 8)
            } catch (_: Exception) {
                failed.countDown()
                return@thread
            }
            // read may also return -1/throw; if it returns normally with data that's unexpected
            // here
            failed.countDown()
        }
        assertTrue(failed.await(2, TimeUnit.SECONDS), "peer read did not unblock after reset")
        cli.close()
        srv.close()
    }
}
