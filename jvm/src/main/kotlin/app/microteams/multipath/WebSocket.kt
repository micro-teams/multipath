/*
 * A minimal RFC 6455 WebSocket, server side, just enough to accept a link that arrives over a CDN
 * edge as an HTTP upgrade. A link is an opaque byte stream, so the only opcode that matters is
 * binary; control frames (ping/pong/close) are handled just enough to keep the stream honest. No
 * extensions, no compression, no text frames.
 *
 * In-package rather than a dependency because the transport ships with none: a WebSocket used only
 * as a byte pipe is a page of framing, not a reason to take a library whose message semantics we do
 * not want. It exposes plain InputStream/OutputStream, so everything above a link is unchanged
 * whether the link is raw TLS or a WebSocket. Wire-compatible with the Go peer (websocket.go).
 */
package app.microteams.multipath

import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.DataInputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import java.util.Base64

private const val WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
private const val OP_BINARY = 0x2
private const val OP_CLOSE = 0x8
private const val OP_PING = 0x9
private const val OP_PONG = 0xA

/** Computes the Sec-WebSocket-Accept token for a client key. */
internal fun wsAccept(key: String): String {
    val sha1 =
        MessageDigest.getInstance("SHA-1").digest((key + WS_GUID).toByteArray(Charsets.US_ASCII))
    return Base64.getEncoder().encodeToString(sha1)
}

/**
 * Performs the server handshake on a link whose first bytes are an HTTP upgrade request, then
 * returns a byte-stream over binary frames. [inp] must be positioned at the request line; [path],
 * if non-empty, must match what the client requested.
 */
internal fun acceptWebSocket(inp: InputStream, out: OutputStream, path: String): WebSocketConn {
    val reader = BufferedReader(inp.reader(Charsets.ISO_8859_1))
    val requestLine =
        reader.readLine() ?: throw EOFException("multipath: websocket accept: empty request")
    val parts = requestLine.split(" ")
    var key: String? = null
    var upgrade = false
    while (true) {
        val line = reader.readLine() ?: break
        if (line.isEmpty()) break
        val idx = line.indexOf(':')
        if (idx < 0) continue
        val name = line.substring(0, idx).trim().lowercase()
        val value = line.substring(idx + 1).trim()
        when (name) {
            "sec-websocket-key" -> key = value
            "upgrade" -> upgrade = value.equals("websocket", ignoreCase = true)
        }
    }
    if (
        parts.size < 2 ||
            parts[0] != "GET" ||
            !upgrade ||
            key == null ||
            (path.isNotEmpty() && parts[1] != path)
    ) {
        out.write("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n".toByteArray())
        out.flush()
        throw IOException("multipath: not a websocket upgrade for \"$path\"")
    }
    val response =
        "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: ${wsAccept(key)}\r\n\r\n"
    out.write(response.toByteArray())
    out.flush()
    // The BufferedReader may have pulled bytes past the blank line into its own buffer; RFC 6455
    // forbids the client from sending frame data before the handshake completes, so there are none,
    // and reading frames straight off the raw stream is safe.
    return WebSocketConn(inp, out)
}

/**
 * A byte-stream over a server-side WebSocket: reads span binary frames, each write is one binary
 * frame (unmasked, as a server must), and control frames are absorbed. Exposes InputStream and
 * OutputStream so it slots in wherever a plaintext link's streams would.
 */
class WebSocketConn(rawIn: InputStream, private val rawOut: OutputStream) : Closeable {
    private val data = DataInputStream(rawIn)
    private var pending = ByteArray(0)
    private var pendingOff = 0
    private val writeLock = Any()
    @Volatile private var closed = false

    val input: InputStream =
        object : InputStream() {
            override fun read(): Int {
                val one = ByteArray(1)
                return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xff
            }

            override fun read(b: ByteArray, off: Int, len: Int): Int {
                while (pendingOff >= pending.size) {
                    if (!readFrame()) return -1
                }
                val n = minOf(len, pending.size - pendingOff)
                System.arraycopy(pending, pendingOff, b, off, n)
                pendingOff += n
                return n
            }
        }

    val output: OutputStream =
        object : OutputStream() {
            override fun write(b: Int) = writeFrame(OP_BINARY, byteArrayOf(b.toByte()))

            override fun write(b: ByteArray, off: Int, len: Int) =
                writeFrame(OP_BINARY, b.copyOfRange(off, off + len))

            override fun flush() = rawOut.flush()
        }

    /** Reads one frame's payload into [pending], answering control frames; false on close/EOF. */
    private fun readFrame(): Boolean {
        val b0 =
            try {
                data.readUnsignedByte()
            } catch (_: EOFException) {
                return false
            }
        val opcode = b0 and 0x0f
        val b1 = data.readUnsignedByte()
        val masked = b1 and 0x80 != 0
        var length = (b1 and 0x7f).toLong()
        when (length) {
            126L -> length = data.readUnsignedShort().toLong()
            127L -> length = data.readLong()
        }
        val mask = ByteArray(4)
        if (masked) data.readFully(mask)
        val payload = ByteArray(length.toInt())
        data.readFully(payload)
        if (masked)
            for (i in payload.indices) payload[i] =
                (payload[i].toInt() xor mask[i % 4].toInt()).toByte()

        return when (opcode) {
            OP_BINARY,
            0x0 -> {
                pending = payload
                pendingOff = 0
                true
            }
            OP_PING -> {
                writeFrame(OP_PONG, payload)
                readFrame()
            }
            OP_PONG -> readFrame()
            OP_CLOSE -> {
                writeFrame(OP_CLOSE, ByteArray(0))
                false
            }
            else -> throw IOException("multipath: websocket unexpected opcode $opcode")
        }
    }

    /** Emits one frame, unmasked (server). */
    private fun writeFrame(opcode: Int, payload: ByteArray) {
        synchronized(writeLock) {
            if (closed && opcode != OP_CLOSE) throw IOException("multipath: websocket closed")
            val head = ByteArrayOutputStream()
            head.write(0x80 or opcode)
            val len = payload.size
            when {
                len < 126 -> head.write(len)
                len < 1 shl 16 -> {
                    head.write(126)
                    head.write((len ushr 8) and 0xff)
                    head.write(len and 0xff)
                }
                else -> {
                    head.write(127)
                    for (s in 56 downTo 0 step 8) head.write((len ushr s) and 0xff)
                }
            }
            rawOut.write(head.toByteArray())
            rawOut.write(payload)
            rawOut.flush()
        }
    }

    override fun close() {
        val already = closed
        closed = true
        if (!already) {
            try {
                writeFrame(OP_CLOSE, ByteArray(0))
            } catch (_: Exception) {}
        }
    }
}
