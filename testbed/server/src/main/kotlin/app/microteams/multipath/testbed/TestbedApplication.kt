/*
 *  Description: The origin under test — the smallest server that can prove MultiPath works.
 *
 *               There is deliberately no business logic here, and no de-duplication either: any
 *               de-duplication a test observes must have come from the MultiPath interceptor, or
 *               the test proves nothing. Everything this server offers exists to make one
 *               MultiPath property observable from outside.
 *
 *               Single instance, always. That is the assumption the in-memory de-duplication rests
 *               on, so a testbed that quietly ran two would be testing a system we do not ship.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.testbed

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@SpringBootApplication class TestbedApplication

fun main(args: Array<String>) {
    runApplication<TestbedApplication>(*args)
}

/** What a line looks like to a client. Mirrors `ts/src/registry.ts`. */
data class LineDTO(val id: String, val url: String, val transport: String, val weight: Int)

data class LinesDTO(val lines: List<LineDTO>)

data class ProbeDTO(val serverTimeMs: Long)

data class EchoRequestDTO(val op: String)

/** `count` is the number of times this op actually executed — the assertion every test makes. */
data class EchoDTO(val op: String, val count: Int, val servedBy: String)

/**
 * Every endpoint is CORS-open because a testbed line is a different origin (a different port) from
 * the page, and CORS policy is not what is under test here. A real deployment derives its allowed
 * origin from the forwarded headers instead.
 */
@RestController
@CrossOrigin(origins = ["*"], allowedHeaders = ["*"])
class TestbedController {

    /** One counter per op. Never reset except by the explicit endpoint, so tests stay honest. */
    private val executions = ConcurrentHashMap<String, AtomicInteger>()

    /** Identifies this process, so a test can prove all lines really did reach one instance. */
    private val instanceId = java.util.UUID.randomUUID().toString().take(8)

    /**
     * Liveness and latency probe. Unauthenticated and as close to free as an HTTP round trip gets —
     * a probe that costs anything is a probe that distorts what it measures.
     */
    @GetMapping("/mt/probe") fun probe(): ProbeDTO = ProbeDTO(System.currentTimeMillis())

    /**
     * The line registry. Served from configuration so a test can reshape the topology without
     * rebuilding anything.
     */
    @GetMapping("/mt/lines")
    fun lines(): LinesDTO =
        LinesDTO(
            (System.getenv("TESTBED_LINES") ?: "")
                .split(",")
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { spec ->
                    // "id=url=transport=weight", e.g. "fast=http://localhost:9001=direct=100"
                    val parts = spec.split("=")
                    LineDTO(
                        id = parts[0],
                        url = parts.getOrElse(1) { "" },
                        transport = parts.getOrElse(2) { "test" },
                        weight = parts.getOrElse(3) { "100" }.toInt(),
                    )
                }
        )

    /**
     * The counting write. Increments this op's execution count and reports it.
     *
     * This is what makes "took effect exactly once" an observable fact rather than an article of
     * faith: send the same op down two lines at the same instant and read the count back.
     */
    @PostMapping("/mt/echo")
    fun echo(@RequestBody body: EchoRequestDTO): EchoDTO {
        val count = executions.computeIfAbsent(body.op) { AtomicInteger() }.incrementAndGet()
        return EchoDTO(body.op, count, instanceId)
    }

    /** How many times an op has executed, without executing it. */
    @GetMapping("/mt/count")
    fun count(@RequestParam op: String): EchoDTO =
        EchoDTO(op, executions[op]?.get() ?: 0, instanceId)

    /**
     * A read that takes as long as it is told to. Latency is injected at the line proxies for
     * realism; this exists for the cases where the *origin* must be the slow part, so that a test
     * can tell "the line is slow" apart from "the server is slow".
     */
    @GetMapping("/mt/slow")
    fun slow(@RequestParam(defaultValue = "0") ms: Long): ResponseEntity<ProbeDTO> {
        Thread.sleep(ms.coerceIn(0, 10_000))
        return ResponseEntity.ok(ProbeDTO(System.currentTimeMillis()))
    }

    /** Wipes the counters between specs. */
    @PostMapping("/mt/reset")
    fun reset(): Map<String, String> {
        executions.clear()
        return mapOf("status" to "reset", "instance" to instanceId)
    }
}
