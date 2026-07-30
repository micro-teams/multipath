/*
 *  Description: The fake application the filter is tested against.
 *
 *               It counts its own executions and can be held mid-flight on demand — that is the
 *               whole apparatus. Notice what it does *not* contain: nothing about idempotency, no
 *               annotation, no injected store. If a test passes, it passes because the filter did
 *               the work, which is the property the design claims.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@SpringBootApplication class TestApplication

data class WriteRequest(val note: String = "")

@RestController
class GuardedController {

    /** Executions per endpoint. The number every assertion is ultimately about. */
    val executions = ConcurrentHashMap<String, AtomicInteger>()

    /** Released by a test to let a deliberately-slow handler finish. */
    @Volatile var gate: CountDownLatch? = null

    /** Signals that a held handler has actually been entered, so a test never races the winner. */
    @Volatile var entered: CountDownLatch? = null

    fun count(endpoint: String): Int = executions[endpoint]?.get() ?: 0

    fun reset() {
        executions.clear()
        gate = null
        entered = null
    }

    private fun record(endpoint: String): Int =
        executions.computeIfAbsent(endpoint) { AtomicInteger() }.incrementAndGet()

    @PostMapping("/write")
    fun write(@RequestBody body: WriteRequest): Map<String, Any> {
        val n = record("write")
        entered?.countDown()
        gate?.await(10, TimeUnit.SECONDS)
        return mapOf("note" to body.note, "executions" to n)
    }

    @PostMapping("/reject")
    fun reject(): ResponseEntity<Map<String, Any>> {
        val n = record("reject")
        // A deterministic rejection: it will say the same thing however many times it is asked,
        // which is what makes it safe to remember.
        return ResponseEntity.badRequest().body(mapOf("error" to "no", "executions" to n))
    }

    @PostMapping("/wobble")
    fun wobble(): ResponseEntity<Map<String, Any>> {
        val n = record("wobble")
        // Fails the first time and succeeds after. Lets a test tell "replayed" apart from
        // "re-executed" by the status alone: a 503 twice means the handler ran once.
        return if (n == 1) {
            ResponseEntity.status(503).body(mapOf("error" to "try later", "executions" to n))
        } else {
            ResponseEntity.ok(mapOf("ok" to true, "executions" to n))
        }
    }

    @PostMapping("/async")
    fun async(): java.util.concurrent.CompletableFuture<Map<String, Any>> =
        java.util.concurrent.CompletableFuture.supplyAsync {
            Thread.sleep(200)
            mapOf("executions" to record("async"))
        }

    @PostMapping("/explode")
    fun explode(): Map<String, Any> {
        record("explode")
        throw IllegalStateException("handler blew up")
    }

    @PostMapping("/big")
    fun big(@RequestParam(defaultValue = "16") bytes: Int): String {
        record("big")
        return "x".repeat(bytes)
    }

    @DeleteMapping("/thing") fun remove(): Map<String, Any> = mapOf("executions" to record("thing"))

    @GetMapping("/read") fun read(): Map<String, Any> = mapOf("executions" to record("read"))
}
