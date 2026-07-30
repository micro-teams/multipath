/*
 *  Description: Configuration for the idempotency filter. Every knob here is a policy question
 *               that a consuming application might legitimately answer differently; anything that
 *               is *not* a policy question is deliberately absent, because a setting nobody should
 *               change is a setting somebody eventually will.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "multipath.idempotency")
data class IdempotencyProperties(

    /** Off means the filter is not registered at all — no cost, not merely no effect. */
    val enabled: Boolean = true,

    /** The request header carrying the client's key for one logical write. */
    val header: String = "Idempotency-Key",

    /**
     * How long a completed result stays replayable.
     *
     * It only has to span the window in which a duplicate can still arrive: a client racing lines,
     * plus its retries. Minutes, not hours — a longer TTL does not make anything safer, it just
     * keeps answers around long enough to be surprising.
     */
    val ttl: Duration = Duration.ofMinutes(5),

    /**
     * Methods to guard. GET and HEAD are absent because they are already idempotent, and PUT
     * because a well-formed PUT is too — including them would add a de-duplication window to
     * requests that never needed one.
     */
    val methods: Set<String> = setOf("POST", "PATCH", "DELETE"),

    /**
     * How long a duplicate waits for the first request to finish before giving up.
     *
     * It must exceed the slowest handler being guarded. A duplicate that gives up is told to retry;
     * it is never let through to the handler, because "the first one is taking a while" is not a
     * reason to execute the write twice.
     */
    val wait: Duration = Duration.ofSeconds(30),

    /**
     * Responses larger than this are served but not stored, and the key is released.
     *
     * A duplicate then re-executes rather than being replayed — worse, but bounded. The alternative
     * is letting one oversized response decide how much heap the de-duplication layer takes.
     */
    val maxResponseBytes: Int = 1024 * 1024,
)
