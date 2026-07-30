/*
 *  Description: What MP-2 either does or does not do.
 *
 *               These run against a real embedded server with real worker threads, not MockMvc.
 *               The central claim — two arrivals at the same instant execute once — is a claim
 *               about threads racing, and MockMvc's single-threaded dispatch cannot fail it. A test
 *               that cannot fail is not evidence.
 *
 *               No database, no docker, seconds to run: the whole point of pushing de-duplication
 *               into the transport layer is that it becomes testable on its own.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.test.context.TestPropertySource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(
    properties =
        [
            "multipath.idempotency.ttl=2s",
            "multipath.idempotency.wait=5s",
            "multipath.idempotency.max-response-bytes=64",
            "server.tomcat.threads.max=50",
        ]
)
class IdempotencyFilterTest {

    @Autowired private lateinit var rest: TestRestTemplate
    @Autowired private lateinit var controller: GuardedController
    @LocalServerPort private var port: Int = 0

    @BeforeEach
    fun clean() {
        controller.reset()
    }

    private fun post(
        path: String,
        key: String?,
        body: Any = WriteRequest("hello"),
    ): ResponseEntity<String> {
        val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
        key?.let { headers.set("Idempotency-Key", it) }
        return rest.exchange(
            "http://localhost:$port$path",
            HttpMethod.POST,
            HttpEntity(body, headers),
            String::class.java,
        )
    }

    // ---- the case the whole component exists for --------------------------------------------

    /**
     * Two lines deliver the same write at the same instant.
     *
     * The requests are released together from a starting gate, and the handler is held open until
     * every one of them has arrived — so they genuinely overlap rather than merely being issued in
     * a loop. Under a check-then-claim implementation this is the test that fails.
     */
    @Test
    fun `simultaneous duplicates execute exactly once and all get the same answer`() {
        val arrivals = 8
        val gate = CountDownLatch(1)
        val handlerHeld = CountDownLatch(1)
        controller.gate = handlerHeld
        controller.entered = CountDownLatch(1)

        val pool = Executors.newFixedThreadPool(arrivals)
        try {
            val responses =
                (1..arrivals)
                    .map {
                        pool.submit<ResponseEntity<String>> {
                            gate.await(10, TimeUnit.SECONDS)
                            post("/write", "same-key")
                        }
                    }
                    .also { gate.countDown() }
                    // Let every duplicate pile up behind the winner before the winner is allowed
                    // to finish, so this measures the concurrent case and not a sequence of
                    // already-completed ones.
                    .also {
                        assertTrue(controller.entered!!.await(10, TimeUnit.SECONDS))
                        Thread.sleep(300)
                        handlerHeld.countDown()
                    }
                    .map { it.get(20, TimeUnit.SECONDS) }

            assertEquals(1, controller.count("write"), "the handler must have run exactly once")

            val bodies = responses.map { it.body }.toSet()
            assertEquals(1, bodies.size, "every caller must see byte-identical output")
            responses.forEach { assertEquals(200, it.statusCode.value()) }

            // Exactly one caller was the original; everyone else was replayed.
            assertEquals(
                arrivals - 1,
                responses.count { it.headers.getFirst(IdempotencyFilter.REPLAY_HEADER) == "true" },
            )
        } finally {
            pool.shutdownNow()
        }
    }

    /** The same thing again, sequentially — the ordinary retry, as opposed to the race. */
    @Test
    fun `a later duplicate is replayed rather than re-executed`() {
        val first = post("/write", "retry-key")
        val second = post("/write", "retry-key")

        assertEquals(1, controller.count("write"))
        assertEquals(first.body, second.body)
        assertNull(first.headers.getFirst(IdempotencyFilter.REPLAY_HEADER))
        assertEquals("true", second.headers.getFirst(IdempotencyFilter.REPLAY_HEADER))
    }

    /**
     * Replay, not rejection.
     *
     * A duplicate is a real client still waiting for an answer. Telling it "duplicate" would report
     * failure for a write that succeeded, and it would then retry — de-duplication turning into a
     * duplicate generator.
     */
    @Test
    fun `the replayed answer is the original success, not an error about duplication`() {
        val original = post("/write", "replay-key")
        val duplicate = post("/write", "replay-key")

        assertEquals(200, duplicate.statusCode.value())
        assertTrue(duplicate.body!!.contains("\"executions\":1"))
        assertEquals(original.body, duplicate.body)
    }

    // ---- which outcomes are worth remembering ------------------------------------------------

    @Test
    fun `a deterministic rejection is remembered, because it would say the same thing again`() {
        val first = post("/reject", "reject-key")
        val second = post("/reject", "reject-key")

        assertEquals(400, first.statusCode.value())
        assertEquals(400, second.statusCode.value())
        assertEquals(1, controller.count("reject"))
        assertEquals("true", second.headers.getFirst(IdempotencyFilter.REPLAY_HEADER))
    }

    /**
     * A failure is the attempt's outcome, so it is replayed like any other.
     *
     * The transport does not get to decide that some failures deserve another go. A key names one
     * attempt; whoever mints keys decides whether there is a second attempt, and a business-level
     * retry mints a new one and really executes. Re-running here would be the transport quietly
     * making that decision on the caller's behalf.
     */
    @Test
    fun `a 5xx is replayed like anything else`() {
        val first = post("/wobble", "wobble-key")
        assertEquals(503, first.statusCode.value())

        val duplicate = post("/wobble", "wobble-key")
        assertEquals(503, duplicate.statusCode.value())
        assertEquals(1, controller.count("wobble"), "the same attempt must not run twice")
        assertEquals("true", duplicate.headers.getFirst(IdempotencyFilter.REPLAY_HEADER))
    }

    /** A fresh key is a fresh attempt, and that is how a retry actually happens. */
    @Test
    fun `a new key after a failure really re-executes`() {
        assertEquals(503, post("/wobble", "attempt-1").statusCode.value())
        assertEquals(200, post("/wobble", "attempt-2").statusCode.value())
        assertEquals(2, controller.count("wobble"))
    }

    /**
     * An async controller returns a promise, not an answer.
     *
     * `chain.doFilter` comes back as soon as the handler has said "later", so a filter that settles
     * there stores an empty body and — worse — flushes that emptiness to the *original* caller, who
     * did nothing wrong. The whole request has to be seen through its second dispatch.
     */
    @Test
    fun `an async handler's real body is returned and replayed`() {
        val first = post("/async", "async-key")
        assertEquals(200, first.statusCode.value())
        assertTrue(
            first.body!!.contains("\"executions\":1"),
            "the original caller must get the real body, not an empty 200: was ${first.body}",
        )

        val duplicate = post("/async", "async-key")
        assertEquals(first.body, duplicate.body)
        assertEquals("true", duplicate.headers.getFirst(IdempotencyFilter.REPLAY_HEADER))
        assertEquals(1, controller.count("async"))
    }

    @Test
    fun `a handler that throws releases the key instead of poisoning it`() {
        assertEquals(500, post("/explode", "boom-key").statusCode.value())
        // The key is free again: a client whose request died mid-flight must be able to retry.
        assertEquals(500, post("/explode", "boom-key").statusCode.value())
        assertEquals(2, controller.count("explode"))
    }

    @Test
    fun `an oversized response is served but not stored`() {
        val big =
            rest.postForEntity("http://localhost:$port/big?bytes=500", null, String::class.java)
        assertEquals(500, big.body!!.length)

        val headers = HttpHeaders().apply { set("Idempotency-Key", "big-key") }
        val first =
            rest.exchange(
                "http://localhost:$port/big?bytes=500",
                HttpMethod.POST,
                HttpEntity<Void>(headers),
                String::class.java,
            )
        assertEquals(200, first.statusCode.value())
        assertEquals(500, first.body!!.length, "the caller still gets the whole response")

        val second =
            rest.exchange(
                "http://localhost:$port/big?bytes=500",
                HttpMethod.POST,
                HttpEntity<Void>(headers),
                String::class.java,
            )
        assertNull(
            second.headers.getFirst(IdempotencyFilter.REPLAY_HEADER),
            "too large to keep, so the duplicate re-executes rather than being replayed",
        )
    }

    // ---- staying out of the way --------------------------------------------------------------

    @Test
    fun `without a key the filter is completely transparent`() {
        post("/write", null)
        post("/write", null)
        assertEquals(2, controller.count("write"))
    }

    @Test
    fun `reads are never guarded`() {
        val headers = HttpHeaders().apply { set("Idempotency-Key", "read-key") }
        repeat(2) {
            rest.exchange(
                "http://localhost:$port/read",
                HttpMethod.GET,
                HttpEntity<Void>(headers),
                String::class.java,
            )
        }
        assertEquals(2, controller.count("read"))
    }

    @Test
    fun `DELETE is guarded too`() {
        val headers = HttpHeaders().apply { set("Idempotency-Key", "delete-key") }
        repeat(2) {
            rest.exchange(
                "http://localhost:$port/thing",
                HttpMethod.DELETE,
                HttpEntity<Void>(headers),
                String::class.java,
            )
        }
        assertEquals(1, controller.count("thing"))
    }

    /**
     * The key is scoped by method and path.
     *
     * A client that accidentally reuses a key across two endpoints should collide with itself, not
     * be handed one endpoint's answer in response to a call to the other — which would be a
     * correctness bug wearing the costume of a cache hit.
     */
    @Test
    fun `the same key on a different endpoint is a different key`() {
        post("/write", "shared")
        val other = post("/reject", "shared")

        assertEquals(400, other.statusCode.value(), "must not be served /write's 200")
        assertEquals(1, controller.count("write"))
        assertEquals(1, controller.count("reject"))
    }

    @Test
    fun `after the TTL the same key executes again`() {
        post("/write", "expiring")
        assertEquals(1, controller.count("write"))

        Thread.sleep(2_500) // ttl is 2s for this test
        post("/write", "expiring")
        assertEquals(2, controller.count("write"))
    }

    @Test
    fun `distinct keys never interfere`() {
        val first = post("/write", "key-a")
        val second = post("/write", "key-b")

        assertEquals(2, controller.count("write"))
        assertNotNull(first.body)
        assertTrue(second.body!!.contains("\"executions\":2"))
    }
}
