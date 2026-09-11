package app.microteams.multipath.redundant

import java.net.ServerSocket
import java.net.Socket
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PortMapTest {

    // Reuse a chunk-queue in-memory transport (same shape as MuxTest's).
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
                    q.put(eof)
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

    private fun sessionPair(): Pair<MuxSession, MuxSession> {
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
        return MuxSession.client(wrap(bToA, aToB)) to MuxSession.server(wrap(aToB, bToA))
    }

    private fun startEchoTarget(): ServerSocket {
        val ln = ServerSocket(0)
        thread(isDaemon = true) {
            while (true) {
                val c =
                    try {
                        ln.accept()
                    } catch (_: Exception) {
                        return@thread
                    }
                thread(isDaemon = true) {
                    try {
                        val i = c.getInputStream()
                        val o = c.getOutputStream()
                        val buf = ByteArray(32 * 1024)
                        while (true) {
                            val n = i.read(buf)
                            if (n < 0) break
                            o.write(buf, 0, n)
                            o.flush()
                        }
                    } catch (_: Exception) {}
                    try {
                        c.close()
                    } catch (_: Exception) {}
                }
            }
        }
        return ln
    }

    @Test
    fun forwardsManyConnectionsToTarget() {
        val echo = startEchoTarget()
        val (cli, srv) = sessionPair()
        thread(isDaemon = true) {
            PortMap.serve(srv, PortMap.forwardTo { Socket("127.0.0.1", echo.localPort) })
        }
        val local = ServerSocket(0)
        thread(isDaemon = true) { PortMap.forward(local, cli) }

        val n = 20
        val latch = CountDownLatch(n)
        val failure = AtomicReference<String?>(null)
        for (i in 0 until n) {
            thread(isDaemon = true) {
                try {
                    Socket("127.0.0.1", local.localPort).use { c ->
                        val msg =
                            ("conn-$i:".toByteArray()) +
                                ByteArray(8 * 1024).also { Random((i + 1).toLong()).nextBytes(it) }
                        c.getOutputStream().write(msg)
                        c.getOutputStream().flush()
                        val got = ByteArray(msg.size)
                        var have = 0
                        val inp = c.getInputStream()
                        while (have < msg.size) {
                            val r = inp.read(got, have, msg.size - have)
                            if (r < 0) break
                            have += r
                        }
                        if (have != msg.size || !got.contentEquals(msg))
                            failure.set("conn $i mismatch ($have/${msg.size})")
                    }
                } catch (e: Exception) {
                    failure.set("conn $i err: $e")
                } finally {
                    latch.countDown()
                }
            }
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS), "connections did not finish")
        assertEquals(null, failure.get())
        cli.close()
        srv.close()
        local.close()
        echo.close()
    }

    @Test
    fun customHandlerConsumesStream() {
        val (cli, srv) = sessionPair()
        thread(isDaemon = true) {
            PortMap.serve(srv) { st ->
                // read until EOF, reply with the count
                val all = ArrayList<Byte>()
                val buf = ByteArray(4096)
                while (true) {
                    val n = st.read(buf, 0, buf.size)
                    if (n < 0) break
                    for (k in 0 until n) all.add(buf[k])
                }
                st.write("got ${all.size} bytes".toByteArray())
                st.closeWrite()
            }
        }
        val local = ServerSocket(0)
        thread(isDaemon = true) { PortMap.forward(local, cli) }

        Socket("127.0.0.1", local.localPort).use { c ->
            val payload = ByteArray(5000).also { Random(9).nextBytes(it) }
            c.getOutputStream().write(payload)
            c.getOutputStream().flush()
            c.shutdownOutput()
            val got = c.getInputStream().readBytes()
            assertEquals("got ${payload.size} bytes", String(got))
        }
        cli.close()
        srv.close()
        local.close()
    }
}
