// Redundant multi-link byte stream — the JVM peer of go/redundant.go, speaking the identical wire
// format so a Go client and a JVM server interoperate. Aggregates N unreliable links into one
// reliable, ordered, never-interrupted duplex byte stream: every byte is written to all live links,
// the receiver delivers each once in order (first-arriving link wins, the rest de-duplicated by
// absolute offset), and a dead link reconnects (client) or is re-attached (server) and replays the
// unacknowledged buffer invisibly to the caller.
//
// The public surface is deliberately just read()/write()/close() plus InputStream/OutputStream —
// the
// caller never learns that more than one link exists.

package app.microteams.multipath.redundant

import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.security.SecureRandom
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

/**
 * Reported to [RedundantOptions.onLinkState] the moment a line's up/down state changes — the edge a
 * log wants. [up] is true on (re)connect, false on drop; [reason] is the drop cause (empty on up);
 * [durationMs] is how long the line spent in the state it just left.
 */
data class LinkState(val index: Int, val up: Boolean, val reason: String, val durationMs: Long)

/**
 * A point-in-time snapshot of one line — the level a status view wants. [state] is "up",
 * "connecting" (never yet up) or "down" (was up, now reconnecting). [lastByteMs] is when a frame
 * last arrived (0 if never). [reconnects] counts recoveries after a drop; [reason] is the last drop
 * cause.
 */
data class LinkStat(
    val index: Int,
    val state: String,
    val lastByteMs: Long,
    val reconnects: Int,
    val reason: String,
)

/** Tunables; zero/absent values take the defaults (mirroring the Go RedundantOptions). */
data class RedundantOptions(
    val n: Int,
    val window: Int = 4 shl 20,
    val pingIntervalMs: Long = 5000,
    val deadAfterMs: Long = 15000,
    val ackIntervalMs: Long = 50,
    val reconnectDelayMs: Long = 200,
    val maxDelayMs: Long = 5000,
    // Called once per line up/down transition (never on the hot path, never under the internal
    // lock).
    // Edge-triggered: repeated failed reconnects don't re-fire it, but the latest reason shows up
    // in
    // stats(). Optional.
    val onLinkState: ((LinkState) -> Unit)? = null,
)

/** One underlying link: its streams plus a write lock so frames never interleave on the wire. */
class LinkConn(val input: InputStream, val output: OutputStream, private val closer: Closeable) {
    private val writeLock = Any()

    fun write(frame: ByteArray): Boolean =
        try {
            synchronized(writeLock) {
                output.write(frame)
                output.flush()
            }
            true
        } catch (_: Exception) {
            false
        }

    fun close() {
        try {
            closer.close()
        } catch (_: Exception) {}
    }
}

/**
 * A growable byte buffer with an advancing head, used for the send buffer and the delivery inbox.
 */
internal class ByteQueue {
    private var buf = ByteArray(1024)
    private var head = 0
    var size = 0
        private set

    fun append(src: ByteArray, off: Int, len: Int) {
        ensure(len)
        System.arraycopy(src, off, buf, head + size, len)
        size += len
    }

    fun trimFront(k: Int) {
        val t = if (k > size) size else k
        head += t
        size -= t
        if (size == 0) head = 0
    }

    /** Copies the whole live range out. */
    fun snapshot(): ByteArray = buf.copyOfRange(head, head + size)

    /** Drains up to [max] bytes from the front into [dst] at [dstOff]; returns the count. */
    fun drain(dst: ByteArray, dstOff: Int, max: Int): Int {
        val n = if (max < size) max else size
        System.arraycopy(buf, head, dst, dstOff, n)
        trimFront(n)
        return n
    }

    private fun ensure(extra: Int) {
        if (head + size + extra <= buf.size) return
        if (size + extra <= buf.size) {
            // Compact in place.
            System.arraycopy(buf, head, buf, 0, size)
            head = 0
            return
        }
        var cap = buf.size * 2
        while (cap < size + extra) cap *= 2
        val nb = ByteArray(cap)
        System.arraycopy(buf, head, nb, 0, size)
        buf = nb
        head = 0
    }
}

/** A reliable, ordered, duplex byte stream carried redundantly over N links. */
class RedundantStream
internal constructor(
    private val opt: RedundantOptions,
    private val client: Boolean,
    private val connId: ByteArray,
    private val dialer: ((Int) -> LinkConn?)?,
) : Closeable {
    private val lock = ReentrantLock()
    private val readable = lock.newCondition()
    private val writable = lock.newCondition()

    // Send side.
    private var sendBase = 0L
    private var sendNext = 0L
    private val sendBuf = ByteQueue()

    // Receive side.
    private var deliverNext = 0L
    private val reasm = HashMap<Long, ByteArray>()
    private val inbox = ByteQueue()

    private val links = arrayOfNulls<LinkConn>(opt.n)
    private val lastSeen = LongArray(opt.n) { System.nanoTime() }
    private var closed = false

    // Per-link observability (see LinkState/LinkStat). state ∈ {"up","connecting","down"}.
    private val linkState = Array(opt.n) { "connecting" }
    private val linkSince = LongArray(opt.n) { System.currentTimeMillis() }
    private val linkReconn = IntArray(opt.n)
    private val linkReason = arrayOfNulls<String>(opt.n)
    private val lastByteMs = LongArray(opt.n)
    private val reapReason = arrayOfNulls<String>(opt.n)

    // markUp records link i as connected, firing onLinkState on a real transition. Must not hold
    // lock.
    private fun markUp(i: Int) {
        var fire: LinkState? = null
        lock.withLock {
            if (i < 0 || i >= linkState.size || linkState[i] == "up") return
            val was = linkState[i]
            val dur = System.currentTimeMillis() - linkSince[i]
            linkState[i] = "up"
            linkSince[i] = System.currentTimeMillis()
            if (was == "down") linkReconn[i]++
            fire = LinkState(i, true, "", dur)
        }
        fire?.let { opt.onLinkState?.invoke(it) }
    }

    // markDown records link i as down with a reason; fires onLinkState only on a real transition,
    // so
    // repeated failed reconnects don't spam. Must not hold lock.
    private fun markDown(i: Int, reason: String) {
        var fire: LinkState? = null
        lock.withLock {
            if (i < 0 || i >= linkState.size) return
            linkReason[i] = reason
            if (linkState[i] == "down") return
            val dur = System.currentTimeMillis() - linkSince[i]
            linkState[i] = "down"
            linkSince[i] = System.currentTimeMillis()
            fire = LinkState(i, false, reason, dur)
        }
        fire?.let { opt.onLinkState?.invoke(it) }
    }

    /**
     * A snapshot of every line's current health. A status view reads this; a log uses onLinkState.
     */
    fun stats(): List<LinkStat> =
        lock.withLock {
            (0 until opt.n).map { i ->
                LinkStat(i, linkState[i], lastByteMs[i], linkReconn[i], linkReason[i] ?: "")
            }
        }

    internal fun start() {
        thread(isDaemon = true, name = "mp-keepalive") { keepaliveLoop() }
        thread(isDaemon = true, name = "mp-ack") { ackLoop() }
    }

    // --- client link management ---

    internal fun connectAll() {
        for (i in 0 until opt.n) {
            val li = i
            thread(isDaemon = true, name = "mp-dial-$li") { connectLink(li, firstTry = false) }
        }
    }

    private fun connectLink(i: Int, firstTry: Boolean) {
        var delay = opt.reconnectDelayMs
        while (!isClosed()) {
            val conn =
                try {
                    dialer?.invoke(i)
                } catch (e: Exception) {
                    markDown(i, "dial: ${e.message ?: e.javaClass.simpleName}")
                    null
                }
            if (conn != null) {
                if (conn.write(encodeHello(connId, i)) && attachLink(i, conn)) return
                if (isClosed()) return
            } else {
                markDown(i, linkReason[i] ?: "dial failed")
            }
            if (firstTry) return
            try {
                Thread.sleep(delay)
            } catch (_: InterruptedException) {
                return
            }
            delay = (delay * 2).coerceAtMost(opt.maxDelayMs)
        }
    }

    /** Installs conn as link i, replays the unacknowledged buffer, and starts its reader. */
    internal fun attachLink(i: Int, conn: LinkConn): Boolean {
        val replay: ByteArray
        val base: Long
        lock.withLock {
            if (closed) {
                conn.close()
                return false
            }
            links[i]?.close()
            links[i] = conn
            lastSeen[i] = System.nanoTime()
            lastByteMs[i] = System.currentTimeMillis()
            base = sendBase
            replay = sendBuf.snapshot()
        }
        var off = 0
        while (off < replay.size) {
            val len = (replay.size - off).coerceAtMost(MAX_SEGMENT)
            if (!conn.write(encodeData(base + off, replay, off, len))) break
            off += len
        }
        thread(isDaemon = true, name = "mp-read-$i") { readLoop(i, conn) }
        markUp(i)
        return true
    }

    private fun readLoop(i: Int, conn: LinkConn) {
        val r = FrameReader(conn.input)
        var readErr: String? = null
        try {
            while (true) {
                val f = r.next()
                when (f.type) {
                    FRAME_DATA -> onData(f.offset, f.payload!!)
                    FRAME_ACK -> onAck(f.offset)
                    FRAME_PING -> conn.write(encodeNonce(FRAME_PONG, f.nonce))
                    FRAME_PONG -> {}
                    FRAME_HELLO -> {}
                }
                touch(i)
            }
        } catch (e: Exception) {
            readErr = e.message ?: e.javaClass.simpleName
        }
        val reconnect: Boolean
        val closing: Boolean
        val hint: String?
        lock.withLock {
            if (links[i] === conn) links[i] = null
            closing = closed
            reconnect = !closed && client
            hint = reapReason[i]
            reapReason[i] = null
        }
        conn.close()
        if (!closing) markDown(i, hint ?: readErr ?: "link closed")
        if (reconnect)
            thread(isDaemon = true, name = "mp-reconnect-$i") { connectLink(i, firstTry = false) }
    }

    private fun touch(i: Int) =
        lock.withLock {
            lastSeen[i] = System.nanoTime()
            lastByteMs[i] = System.currentTimeMillis()
        }

    // --- receive side ---

    private fun onData(off0: Long, payload0: ByteArray) {
        if (payload0.isEmpty()) return
        lock.withLock {
            var off = off0
            var payload = payload0
            val end = off + payload.size
            if (end <= deliverNext) return // wholly old — first link already won; drop duplicate
            if (off < deliverNext) {
                val skip = (deliverNext - off).toInt()
                payload = payload.copyOfRange(skip, payload.size)
                off = deliverNext
            }
            if (off > deliverNext) {
                val prev = reasm[off]
                if (prev == null || payload.size > prev.size) reasm[off] = payload
                return
            }
            inbox.append(payload, 0, payload.size)
            deliverNext = end
            drainReasm()
            readable.signalAll()
        }
    }

    private fun drainReasm() {
        while (true) {
            val seg = reasm.remove(deliverNext)
            if (seg != null) {
                inbox.append(seg, 0, seg.size)
                deliverNext += seg.size
                continue
            }
            var advanced = false
            val it = reasm.entries.iterator()
            while (it.hasNext()) {
                val (start, buf) = it.next()
                val bend = start + buf.size
                if (start < deliverNext && bend > deliverNext) {
                    val skip = (deliverNext - start).toInt()
                    inbox.append(buf, skip, buf.size - skip)
                    deliverNext = bend
                    it.remove()
                    advanced = true
                    break
                }
                if (bend <= deliverNext) it.remove()
            }
            if (!advanced) return
        }
    }

    // --- send side ---

    private fun onAck(cumulative: Long) =
        lock.withLock {
            if (cumulative > sendBase) {
                sendBuf.trimFront((cumulative - sendBase).toInt())
                sendBase = cumulative
                writable.signalAll()
            }
        }

    fun write(p: ByteArray) {
        var pos = 0
        while (pos < p.size) {
            val frame: ByteArray
            val live = ArrayList<LinkConn>(opt.n)
            lock.withLock {
                while (!closed && sendBuf.size >= opt.window) writable.await()
                if (closed) throw java.io.IOException("multipath: redundant stream closed")
                val room = opt.window - sendBuf.size
                val n = (p.size - pos).coerceAtMost(room).coerceAtMost(MAX_SEGMENT)
                val off = sendNext
                sendBuf.append(p, pos, n)
                sendNext += n
                frame = encodeData(off, p, pos, n)
                for (l in links) if (l != null) live.add(l)
                pos += n
            }
            for (l in live) l.write(frame)
        }
    }

    /**
     * Reads delivered bytes in order; blocks until at least one is available or the stream closes.
     */
    fun read(dst: ByteArray, off: Int, len: Int): Int {
        lock.withLock {
            while (inbox.size == 0 && !closed) readable.await()
            if (inbox.size == 0 && closed) return -1
            return inbox.drain(dst, off, len)
        }
    }

    override fun close() {
        val snapshot: List<LinkConn>
        lock.withLock {
            if (closed) return
            closed = true
            snapshot = links.filterNotNull()
            for (i in links.indices) links[i] = null
            readable.signalAll()
            writable.signalAll()
        }
        for (l in snapshot) l.close()
    }

    private fun isClosed(): Boolean = lock.withLock { closed }

    fun inputStream(): InputStream =
        object : InputStream() {
            private val one = ByteArray(1)

            override fun read(): Int = if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xFF

            override fun read(b: ByteArray, o: Int, l: Int): Int =
                this@RedundantStream.read(b, o, l)
        }

    fun outputStream(): OutputStream =
        object : OutputStream() {
            override fun write(b: Int) = write(byteArrayOf(b.toByte()))

            override fun write(b: ByteArray, o: Int, l: Int) =
                this@RedundantStream.write(b.copyOfRange(o, o + l))
        }

    // --- keepalive + ack ---

    private fun keepaliveLoop() {
        while (!isClosed()) {
            try {
                Thread.sleep(opt.pingIntervalMs)
            } catch (_: InterruptedException) {
                return
            }
            val now = System.nanoTime()
            val toPing = ArrayList<LinkConn>()
            val toReap = ArrayList<LinkConn>()
            val nonce = now
            lock.withLock {
                for (i in links.indices) {
                    val l = links[i] ?: continue
                    if ((now - lastSeen[i]) / 1_000_000 > opt.deadAfterMs) {
                        toReap.add(l)
                        links[i] = null
                        reapReason[i] = "no data for ${opt.deadAfterMs}ms"
                    } else {
                        toPing.add(l)
                    }
                }
            }
            for (l in toPing) l.write(encodeNonce(FRAME_PING, nonce))
            for (l in toReap) l.close() // its reader errors out and (client) reconnects
        }
    }

    private fun ackLoop() {
        while (!isClosed()) {
            try {
                Thread.sleep(opt.ackIntervalMs)
            } catch (_: InterruptedException) {
                return
            }
            val cum: Long
            val live = ArrayList<LinkConn>()
            lock.withLock {
                cum = deliverNext
                for (l in links) if (l != null) live.add(l)
            }
            val frame = encodeAck(cum)
            for (l in live) l.write(frame)
        }
    }

    companion object {
        private val RNG = SecureRandom()

        /**
         * Dials a client-side redundant stream. [dial] opens link i (called again on each
         * reconnect).
         */
        @JvmStatic
        fun dial(opt: RedundantOptions, dial: (Int) -> LinkConn?): RedundantStream {
            val id = ByteArray(16).also { RNG.nextBytes(it) }
            val s = RedundantStream(opt, client = true, connId = id, dialer = dial)
            s.start()
            s.connectAll()
            return s
        }

        internal fun server(opt: RedundantOptions, connId: ByteArray): RedundantStream {
            val s = RedundantStream(opt, client = false, connId = connId, dialer = null)
            s.start()
            return s
        }
    }
}

/**
 * Accepts redundant links off a ServerSocket and groups them by connID into server-side
 * RedundantStreams. A new connID surfaces a stream via [accept]; a reconnecting link of a known
 * connID re-attaches underneath the existing stream.
 */
class RedundantServer(
    private val serverSocket: ServerSocket,
    private val opt: RedundantOptions,
    // Decapsulates each accepted socket into a link (raw TLS / WebSocket / plaintext) so this class
    // always sees the same thing above it: a byte stream whose first frame is HELLO. The default is
    // plaintext, for a testbed or an origin fronted by something that terminates TLS elsewhere.
    private val decap: (Socket) -> LinkConn = {
        LinkConn(it.getInputStream(), it.getOutputStream(), it)
    },
) : Closeable {
    private val streams = HashMap<String, RedundantStream>()
    private val lock = ReentrantLock()
    private val handoff = SynchronousQueue<RedundantStream>()
    @Volatile private var closed = false

    init {
        thread(isDaemon = true, name = "mp-accept") { loop() }
    }

    private fun loop() {
        while (!closed) {
            val sock =
                try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    return
                }
            thread(isDaemon = true, name = "mp-onlink") { onLink(sock) }
        }
    }

    private fun onLink(sock: Socket) {
        try {
            sock.soTimeout = 10_000
            val conn = decap(sock)
            val reader = FrameReader(conn.input)
            val hello = reader.next()
            if (hello.type != FRAME_HELLO) {
                conn.close()
                return
            }
            sock.soTimeout = 0
            val idx = hello.linkIdx
            if (idx < 0 || idx >= opt.n) {
                conn.close()
                return
            }
            val key = hello.connId!!.joinToString("") { "%02x".format(it) }
            var isNew = false
            val stream =
                lock.withLock {
                    streams.getOrPut(key) {
                        isNew = true
                        RedundantStream.server(opt, hello.connId!!)
                    }
                }
            if (isNew) handoff.put(stream)
            if (!stream.attachLink(idx, conn)) {
                lock.withLock { streams.remove(key) }
            }
        } catch (_: Exception) {
            try {
                sock.close()
            } catch (_: Exception) {}
        }
    }

    /** Blocks for the next new logical stream. */
    fun accept(): RedundantStream = handoff.take()

    override fun close() {
        closed = true
        try {
            serverSocket.close()
        } catch (_: Exception) {}
    }
}
