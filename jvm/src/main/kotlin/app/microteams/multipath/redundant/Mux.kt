// Stream multiplexing over one reliable transport — the JVM peer of go/mux.go, wire-identical so a
// Go end and a JVM end interoperate. Carries many independent logical connections ("streams") over
// a
// single transport (in practice a RedundantStream), so a machine holds ONE bundle of N redundant
// links and multiplexes all its connections over it rather than paying N links per connection.
//
// yamux/smux/HTTP-2-style: SYN opens a stream, DATA carries bytes, FIN half-closes, RST aborts, and
// a
// per-stream credit window (WINDOW_UPDATE) provides flow control so one stream can neither starve
// the
// others nor buffer without bound. DATA is chunked so a long write cannot monopolise the transport.
// The streams share one ordered byte stream, so a stalled transport blocks them together
// (head-of-line coupling) — the deliberate trade for a bounded link count.
//
// Wire framing over the transport (big-endian): type:u8 | streamID:u32 | length:u32 | payload.
//   SYN 0x01 (len 0) | DATA 0x02 (len n) | FIN 0x03 (len 0) | RST 0x04 (len 0) | WINDOW_UPDATE 0x05
// (len 4)
// Client (session opener) uses odd stream IDs, server even.

package app.microteams.multipath.redundant

import java.io.Closeable
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

internal const val MUX_SYN = 0x01
internal const val MUX_DATA = 0x02
internal const val MUX_FIN = 0x03
internal const val MUX_RST = 0x04
internal const val MUX_WINDOW = 0x05

internal const val MUX_HDR_LEN = 1 + 4 + 4
internal const val MUX_MAX_CHUNK = 16 * 1024
internal const val MUX_DEF_WINDOW = 256 * 1024

/** A transport the mux runs over: a reliable ordered duplex byte stream. RedundantStream is one. */
interface MuxTransport : Closeable {
    fun read(dst: ByteArray, off: Int, len: Int): Int // -1 on EOF

    fun write(src: ByteArray)
}

/** Adapts a RedundantStream to MuxTransport. */
fun RedundantStream.asMuxTransport(): MuxTransport =
    object : MuxTransport {
        override fun read(dst: ByteArray, off: Int, len: Int): Int =
            this@asMuxTransport.read(dst, off, len)

        override fun write(src: ByteArray) {
            this@asMuxTransport.write(src)
        }

        override fun close() {
            this@asMuxTransport.close()
        }
    }

class MuxSessionClosedException : IOException("multipath: mux session closed")

class MuxStreamResetException : IOException("multipath: stream reset")

/** Multiplexes many [MuxStream]s over one transport. */
class MuxSession private constructor(private val transport: MuxTransport, client: Boolean) :
    Closeable {
    private val wlock = ReentrantLock() // serialises frame writes
    private val lock = ReentrantLock()
    private val streams = HashMap<Long, MuxStream>()
    private var nextId: Long = if (client) 1 else 2
    private val accept = LinkedBlockingQueue<MuxStream>()
    @Volatile private var closed = false
    @Volatile private var closeErr: IOException? = null

    companion object {
        @JvmStatic fun client(transport: MuxTransport): MuxSession = MuxSession(transport, true)

        @JvmStatic fun server(transport: MuxTransport): MuxSession = MuxSession(transport, false)
    }

    private val acceptClosed = MuxStream(this, -1) // sentinel to unblock accept() on close

    init {
        thread(isDaemon = true, name = "mux-read") { readLoop() }
    }

    fun openStream(): MuxStream {
        val id: Long
        val st: MuxStream
        lock.withLock {
            if (closed) throw err()
            id = nextId
            nextId += 2
            st = MuxStream(this, id)
            streams[id] = st
        }
        writeFrame(MUX_SYN, id, null, 0, 0)
        return st
    }

    fun acceptStream(): MuxStream {
        val st = accept.take()
        if (st === acceptClosed) throw err()
        return st
    }

    override fun close() {
        val snapshot: List<MuxStream>
        lock.withLock {
            if (closed) return
            closed = true
            if (closeErr == null) closeErr = MuxSessionClosedException()
            snapshot = streams.values.toList()
        }
        accept.offer(acceptClosed)
        snapshot.forEach { it.shutdown(MuxSessionClosedException()) }
        try {
            transport.close()
        } catch (_: Exception) {}
    }

    private fun err(): IOException = closeErr ?: MuxSessionClosedException()

    internal fun removeStream(id: Long) = lock.withLock { streams.remove(id) }

    internal fun writeFrame(type: Int, id: Long, payload: ByteArray?, off: Int, len: Int) {
        val hdr = ByteArray(MUX_HDR_LEN)
        hdr[0] = type.toByte()
        putU32(hdr, 1, id)
        putU32(hdr, 5, len.toLong())
        wlock.withLock {
            if (closed) throw err()
            transport.write(hdr)
            if (len > 0) {
                transport.write(
                    if (off == 0 && len == payload!!.size) payload
                    else payload!!.copyOfRange(off, off + len)
                )
            }
        }
    }

    private fun readLoop() {
        val hdr = ByteArray(MUX_HDR_LEN)
        try {
            while (true) {
                readFully(hdr, MUX_HDR_LEN)
                val type = hdr[0].toInt() and 0xFF
                val id = u32(hdr, 1)
                val n = u32(hdr, 5).toInt()
                var payload: ByteArray? = null
                if (n > 0) {
                    payload = ByteArray(n)
                    readFully(payload, n)
                }
                when (type) {
                    MUX_SYN -> onSyn(id)
                    MUX_DATA -> getStream(id)?.deliver(payload!!)
                    MUX_FIN -> getStream(id)?.remoteFin()
                    MUX_RST -> {
                        getStream(id)?.shutdown(MuxStreamResetException())
                        removeStream(id)
                    }
                    MUX_WINDOW ->
                        if (payload != null && payload.size == 4)
                            getStream(id)?.grantSend(u32(payload, 0))
                }
            }
        } catch (e: Exception) {
            fail(if (e is IOException) e else IOException(e))
        }
    }

    private fun readFully(b: ByteArray, len: Int) {
        var got = 0
        while (got < len) {
            val r = transport.read(b, got, len - got)
            if (r < 0) throw EOFException()
            got += r
        }
    }

    private fun onSyn(id: Long) {
        lock.withLock {
            if (closed || streams.containsKey(id)) return
            val st = MuxStream(this, id)
            streams[id] = st
            accept.offer(st)
        }
    }

    private fun getStream(id: Long): MuxStream? = lock.withLock { streams[id] }

    private fun fail(e: IOException) {
        val snapshot: List<MuxStream>
        lock.withLock {
            if (closed) return
            closed = true
            closeErr = e
            snapshot = streams.values.toList()
        }
        accept.offer(acceptClosed)
        snapshot.forEach { it.shutdown(e) }
        try {
            transport.close()
        } catch (_: Exception) {}
    }
}

/** One logical connection within a [MuxSession]. */
class MuxStream internal constructor(private val sess: MuxSession, private val id: Long) {
    private val lock = ReentrantLock()
    private val readable = lock.newCondition()
    private val writable = lock.newCondition()
    private var inbox = ByteArray(0)
    private var inboxOff = 0
    private var sendWin = MUX_DEF_WINDOW
    private var remoteEof = false
    private var localFin = false
    private var closed = false
    private var error: IOException? = null

    internal fun deliver(p: ByteArray) =
        lock.withLock {
            if (closed || error != null) return
            // compact + append
            if (inboxOff > 0) {
                inbox = inbox.copyOfRange(inboxOff, inbox.size)
                inboxOff = 0
            }
            inbox += p
            readable.signalAll()
        }

    internal fun remoteFin() {
        lock.withLock {
            remoteEof = true
            readable.signalAll()
        }
        cleanupIfClosed()
    }

    // Frees a stream's slot once both sides have half-closed. A cleanly finished stream must not be
    // aborted with RST to reclaim it: the peer may still be draining buffered DATA, and an RST
    // discards it. So both-FIN is the graceful teardown, and reset() stays for the abnormal case.
    private fun cleanupIfClosed() {
        val done = lock.withLock { localFin && remoteEof }
        if (done) sess.removeStream(id)
    }

    internal fun grantSend(n: Long) =
        lock.withLock {
            sendWin += n.toInt()
            writable.signalAll()
        }

    internal fun shutdown(e: IOException) =
        lock.withLock {
            if (error == null) error = e
            readable.signalAll()
            writable.signalAll()
        }

    fun read(dst: ByteArray, off: Int, len: Int): Int {
        lock.withLock {
            while (inbox.size - inboxOff == 0 && error == null && !remoteEof) readable.await()
            val avail = inbox.size - inboxOff
            if (avail == 0) {
                error?.let { throw it }
                return -1 // remote EOF, drained
            }
            val n = minOf(len, avail)
            System.arraycopy(inbox, inboxOff, dst, off, n)
            inboxOff += n
            // replenish the peer's send credit for what we consumed
            sess.writeFrame(MUX_WINDOW, id, u32Bytes(n.toLong()), 0, 4)
            return n
        }
    }

    fun write(src: ByteArray) {
        var pos = 0
        while (pos < src.size) {
            val n: Int
            lock.withLock {
                while (sendWin == 0 && error == null && !closed && !localFin) writable.await()
                if (error != null || closed || localFin)
                    throw (error ?: IOException("multipath: stream closed"))
                n = minOf(src.size - pos, sendWin, MUX_MAX_CHUNK)
                sendWin -= n
            }
            sess.writeFrame(MUX_DATA, id, src, pos, n)
            pos += n
        }
    }

    /** Half-close the write side (FIN); reads may continue until the peer's EOF. */
    fun closeWrite() {
        lock.withLock {
            if (localFin || closed) return
            localFin = true
            writable.signalAll()
        }
        sess.writeFrame(MUX_FIN, id, null, 0, 0)
        cleanupIfClosed()
    }

    /** Abort the stream in both directions (RST). */
    fun reset() {
        lock.withLock {
            closed = true
            if (error == null) error = MuxStreamResetException()
            readable.signalAll()
            writable.signalAll()
        }
        sess.removeStream(id)
        try {
            sess.writeFrame(MUX_RST, id, null, 0, 0)
        } catch (_: Exception) {}
    }

    fun inputStream(): InputStream =
        object : InputStream() {
            private val one = ByteArray(1)

            override fun read(): Int =
                if (this@MuxStream.read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xFF

            override fun read(b: ByteArray, o: Int, l: Int): Int = this@MuxStream.read(b, o, l)
        }

    fun outputStream(): OutputStream =
        object : OutputStream() {
            override fun write(b: Int) = this@MuxStream.write(byteArrayOf(b.toByte()))

            override fun write(b: ByteArray, o: Int, l: Int) =
                this@MuxStream.write(b.copyOfRange(o, o + l))
        }
}

private fun putU32(b: ByteArray, i: Int, v: Long) {
    b[i] = (v ushr 24).toByte()
    b[i + 1] = (v ushr 16).toByte()
    b[i + 2] = (v ushr 8).toByte()
    b[i + 3] = v.toByte()
}

private fun u32(b: ByteArray, i: Int): Long {
    var v = 0L
    for (k in 0 until 4) v = (v shl 8) or (b[i + k].toLong() and 0xFF)
    return v
}

private fun u32Bytes(v: Long): ByteArray {
    val b = ByteArray(4)
    putU32(b, 0, v)
    return b
}
