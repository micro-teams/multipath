/*
 * L5 — the one thing said at the start of a mux stream: what it is and, for a tunnel, where it goes.
 *
 * A mux stream is an opaque duplex once open, but the origin has to know what to do with a freshly
 * accepted one before any payload flows. That needs at most three things: the kind of stream, a
 * target address when it is a tunnel, and an opaque ticket the consumer (not this library) uses to
 * authorise egress. So the header is those three, sent once, ahead of the bytes — a SOCKS request
 * line with no round trip.
 *
 * Wire-identical to the Go peer (header.go): version:u8 | kind:u8 | targetLen:u16 | target |
 * ticketLen:u16 | ticket, big-endian.
 */
package app.microteams.multipath

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/** What an accepted mux stream carries. */
enum class StreamKind(val code: Int) {
    /** Application traffic bound for the origin's own service; the address is empty. */
    NORMAL(0),

    /** An opaque tunnel to [Header.target], authorised by the consumer's policy. */
    TUNNEL(1);

    companion object {
        fun of(code: Int): StreamKind =
            entries.firstOrNull { it.code == code }
                ?: throw IOException("multipath: unknown stream kind $code")
    }
}

/** What a client says at the start of a mux stream. */
data class Header(
    val kind: StreamKind,
    /** "host:port" for a tunnel, empty for a normal stream. */
    val target: String = "",
    /** An opaque egress capability the origin's handler interprets; the library does not. */
    val ticket: ByteArray = ByteArray(0),
) {
    override fun equals(other: Any?): Boolean =
        other is Header &&
            kind == other.kind &&
            target == other.target &&
            ticket.contentEquals(other.ticket)

    override fun hashCode(): Int =
        (kind.hashCode() * 31 + target.hashCode()) * 31 + ticket.contentHashCode()
}

/** Reads and writes [Header] on the wire, matching the Go peer byte for byte. */
object StreamHeader {
    private const val VERSION = 1
    private const val MAX_FIELD = 4096

    fun write(out: OutputStream, h: Header) {
        val target = h.target.toByteArray(Charsets.UTF_8)
        require(target.size <= MAX_FIELD && h.ticket.size <= MAX_FIELD) {
            "multipath: stream header field exceeds $MAX_FIELD bytes"
        }
        val buf = java.io.ByteArrayOutputStream(6 + target.size + h.ticket.size)
        buf.write(VERSION)
        buf.write(h.kind.code)
        writeField(buf, target)
        writeField(buf, h.ticket)
        out.write(buf.toByteArray())
        out.flush()
    }

    fun read(inp: InputStream): Header {
        val version = readByte(inp)
        if (version != VERSION)
            throw IOException("multipath: unsupported stream header version $version")
        val kind = StreamKind.of(readByte(inp))
        val target = String(readField(inp), Charsets.UTF_8)
        val ticket = readField(inp)
        return Header(kind, target, ticket)
    }

    private fun writeField(out: OutputStream, field: ByteArray) {
        out.write((field.size ushr 8) and 0xff)
        out.write(field.size and 0xff)
        out.write(field)
    }

    private fun readField(inp: InputStream): ByteArray {
        val n = (readByte(inp) shl 8) or readByte(inp)
        if (n > MAX_FIELD)
            throw IOException("multipath: stream header field length $n exceeds $MAX_FIELD")
        val body = ByteArray(n)
        var read = 0
        while (read < n) {
            val r = inp.read(body, read, n - read)
            if (r < 0) throw EOFException("multipath: short stream header field")
            read += r
        }
        return body
    }

    private fun readByte(inp: InputStream): Int {
        val b = inp.read()
        if (b < 0) throw EOFException("multipath: short stream header")
        return b
    }
}
