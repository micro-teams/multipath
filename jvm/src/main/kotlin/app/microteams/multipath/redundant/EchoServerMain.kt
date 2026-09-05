// A tiny standalone redundant-stream echo server, used by the cross-language end-to-end test in
// testbed/redundant-xlang: a Go client sends a payload through a fault middlebox and this JVM
// server
// echoes every received byte back over the same redundant stream. Proves a real Go-client /
// Java-server pairing over the shared wire format under adverse networks.
//
// Args: <port> <n>. Prints "LISTENING <port>" once ready, then serves one accepted stream.

package app.microteams.multipath.redundant

import java.net.ServerSocket

object EchoServerMain {
    @JvmStatic
    fun main(args: Array<String>) {
        val port = args[0].toInt()
        val n = args[1].toInt()
        val opt =
            RedundantOptions(
                n = n,
                window = 2 shl 20,
                pingIntervalMs = 20,
                deadAfterMs = 100,
                ackIntervalMs = 8,
            )
        val serverSocket = ServerSocket(port)
        val server = RedundantServer(serverSocket, opt)
        println("LISTENING ${serverSocket.localPort}")
        System.out.flush()
        val stream = server.accept()
        val buf = ByteArray(64 * 1024)
        while (true) {
            val m = stream.read(buf, 0, buf.size)
            if (m < 0) break
            stream.write(buf.copyOfRange(0, m))
        }
    }
}
