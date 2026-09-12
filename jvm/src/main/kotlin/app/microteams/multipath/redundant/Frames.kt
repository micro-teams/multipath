// Wire framing for the redundant stream, byte-for-byte identical to the Go implementation
// (go/redundant_frame.go) so the two interoperate. All integers are big-endian.
//
//   HELLO 0x05 | connID[16] | linkIndex:u16
//   DATA  0x01 | offset:u64  | len:u16 | crc32:u32 | payload[len]
//   ACK   0x02 | cumulative:u64
//   PING  0x03 | nonce:u64
//   PONG  0x04 | nonce:u64
//
// HELLO is the first frame a client sends on every link; a server groups links by connID.

package app.microteams.multipath.redundant

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.util.zip.CRC32

internal const val FRAME_DATA = 0x01
internal const val FRAME_ACK = 0x02
internal const val FRAME_PING = 0x03
internal const val FRAME_PONG = 0x04
internal const val FRAME_HELLO = 0x05
internal const val FRAME_REJECT = 0x06 // server→client: link refused, with a reason; do not retry

internal const val MAX_SEGMENT = 32 * 1024
internal const val DATA_HDR_LEN = 1 + 8 + 2 + 4

/** Raised on a CRC/length mismatch or an unknown tag; the caller drops that link. */
class CorruptFrameException : IOException("multipath: corrupt frame")

/** A decoded frame. Only the fields relevant to [type] are meaningful. */
internal class Frame(
    val type: Int,
    val offset: Long = 0, // DATA offset, or ACK cumulative
    val payload: ByteArray? = null, // DATA
    val nonce: Long = 0, // PING/PONG
    val connId: ByteArray? = null, // HELLO (16 bytes)
    val linkIdx: Int = 0, // HELLO
    val reason: String = "", // REJECT
)

internal fun crc32(b: ByteArray): Long {
    val c = CRC32()
    c.update(b)
    return c.value
}

private fun putU16(b: ByteArray, i: Int, v: Int) {
    b[i] = (v ushr 8).toByte()
    b[i + 1] = v.toByte()
}

private fun putU32(b: ByteArray, i: Int, v: Long) {
    b[i] = (v ushr 24).toByte()
    b[i + 1] = (v ushr 16).toByte()
    b[i + 2] = (v ushr 8).toByte()
    b[i + 3] = v.toByte()
}

private fun putU64(b: ByteArray, i: Int, v: Long) {
    for (k in 0 until 8) b[i + k] = (v ushr (56 - 8 * k)).toByte()
}

private fun u16(b: ByteArray, i: Int): Int =
    ((b[i].toInt() and 0xFF) shl 8) or (b[i + 1].toInt() and 0xFF)

private fun u32(b: ByteArray, i: Int): Long {
    var v = 0L
    for (k in 0 until 4) v = (v shl 8) or (b[i + k].toLong() and 0xFF)
    return v
}

private fun u64(b: ByteArray, i: Int): Long {
    var v = 0L
    for (k in 0 until 8) v = (v shl 8) or (b[i + k].toLong() and 0xFF)
    return v
}

internal fun encodeHello(connId: ByteArray, linkIdx: Int): ByteArray {
    require(connId.size == 16)
    val b = ByteArray(1 + 16 + 2)
    b[0] = FRAME_HELLO.toByte()
    System.arraycopy(connId, 0, b, 1, 16)
    putU16(b, 17, linkIdx)
    return b
}

internal fun encodeData(offset: Long, payload: ByteArray, start: Int, len: Int): ByteArray {
    val b = ByteArray(DATA_HDR_LEN + len)
    b[0] = FRAME_DATA.toByte()
    putU64(b, 1, offset)
    putU16(b, 9, len)
    val seg = payload.copyOfRange(start, start + len)
    putU32(b, 11, crc32(seg))
    System.arraycopy(seg, 0, b, DATA_HDR_LEN, len)
    return b
}

internal fun encodeAck(cumulative: Long): ByteArray {
    val b = ByteArray(9)
    b[0] = FRAME_ACK.toByte()
    putU64(b, 1, cumulative)
    return b
}

internal fun encodeNonce(type: Int, nonce: Long): ByteArray {
    val b = ByteArray(9)
    b[0] = type.toByte()
    putU64(b, 1, nonce)
    return b
}

/**
 * Frames a server→client refusal with a UTF-8 reason (bounded so a bad length can't over-allocate).
 */
internal fun encodeReject(reason: String): ByteArray {
    var r = reason.toByteArray(Charsets.UTF_8)
    if (r.size > MAX_SEGMENT) r = r.copyOfRange(0, MAX_SEGMENT)
    val b = ByteArray(3 + r.size)
    b[0] = FRAME_REJECT.toByte()
    putU16(b, 1, r.size)
    System.arraycopy(r, 0, b, 3, r.size)
    return b
}

/** Reads whole frames off a link, reassembling across arbitrary chunk boundaries. */
internal class FrameReader(private val input: InputStream) {
    private fun readFully(b: ByteArray) {
        var n = 0
        while (n < b.size) {
            val r = input.read(b, n, b.size - n)
            if (r < 0) throw EOFException()
            n += r
        }
    }

    fun next(): Frame {
        val t = ByteArray(1)
        readFully(t)
        return when (t[0].toInt() and 0xFF) {
            FRAME_HELLO -> {
                val b = ByteArray(18)
                readFully(b)
                Frame(FRAME_HELLO, connId = b.copyOfRange(0, 16), linkIdx = u16(b, 16))
            }
            FRAME_DATA -> {
                val h = ByteArray(DATA_HDR_LEN - 1)
                readFully(h)
                val offset = u64(h, 0)
                val len = u16(h, 8)
                val want = u32(h, 10)
                if (len > MAX_SEGMENT) throw CorruptFrameException()
                val buf = ByteArray(len)
                readFully(buf)
                if (crc32(buf) != want) throw CorruptFrameException()
                Frame(FRAME_DATA, offset = offset, payload = buf)
            }
            FRAME_ACK -> {
                val b = ByteArray(8)
                readFully(b)
                Frame(FRAME_ACK, offset = u64(b, 0))
            }
            FRAME_PING,
            FRAME_PONG -> {
                val b = ByteArray(8)
                readFully(b)
                Frame(t[0].toInt() and 0xFF, nonce = u64(b, 0))
            }
            FRAME_REJECT -> {
                val l = ByteArray(2)
                readFully(l)
                val n = u16(l, 0)
                if (n > MAX_SEGMENT) throw CorruptFrameException()
                val buf = ByteArray(n)
                readFully(buf)
                Frame(FRAME_REJECT, reason = String(buf, Charsets.UTF_8))
            }
            else -> throw CorruptFrameException()
        }
    }
}
