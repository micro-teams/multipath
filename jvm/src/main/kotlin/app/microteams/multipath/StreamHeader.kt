/*
 * L5 — the one thing said at the start of a mux stream: which named service it wants.
 *
 * A mux stream is an opaque duplex once open, but the origin has to know what to do with a freshly
 * accepted one before any payload flows. In this substrate every stream is a request for a named
 * service the origin registered — there is no "main service" and no client-chosen address. The header
 * is therefore just the service name and an opaque ticket its handler interprets, sent once ahead of
 * the bytes. A name the origin has not registered is refused, so a client can never reach an address
 * of its own choosing.
 *
 * Wire-identical to the Go peer (header.go): version:u8 | serviceLen:u16 | service | ticketLen:u16 |
 * ticket, big-endian.
 */
package app.microteams.multipath

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/** What a client says at the start of a mux stream: the registered service it wants. */
data class Header(
    /** The registered service name the stream is for; the origin refuses an unknown name. */
    val service: String,
    /** An opaque capability the service's handler interprets; the library does not. */
    val ticket: ByteArray = ByteArray(0),
) {
    override fun equals(other: Any?): Boolean =
        other is Header && service == other.service && ticket.contentEquals(other.ticket)

    override fun hashCode(): Int = service.hashCode() * 31 + ticket.contentHashCode()
}

/** Reads and writes [Header] on the wire, matching the Go peer byte for byte. */
object StreamHeader {
    private const val VERSION = 2
    private const val MAX_FIELD = 4096

    fun write(out: OutputStream, h: Header) {
        val service = h.service.toByteArray(Charsets.UTF_8)
        require(service.size <= MAX_FIELD && h.ticket.size <= MAX_FIELD) {
            "multipath: stream header field exceeds $MAX_FIELD bytes"
        }
        val buf = java.io.ByteArrayOutputStream(5 + service.size + h.ticket.size)
        buf.write(VERSION)
        writeField(buf, service)
        writeField(buf, h.ticket)
        out.write(buf.toByteArray())
        out.flush()
    }

    fun read(inp: InputStream): Header {
        val version = readByte(inp)
        if (version != VERSION)
            throw IOException("multipath: unsupported stream header version $version")
        val service = String(readField(inp), Charsets.UTF_8)
        val ticket = readField(inp)
        return Header(service, ticket)
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
