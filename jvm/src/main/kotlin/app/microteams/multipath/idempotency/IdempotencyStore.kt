/*
 *  Description: The de-duplication table: one slot per idempotency key, first arrival wins it.
 *
 *               This is the whole concurrency argument, and it is short on purpose. `compute` runs
 *               its function under the map's lock for that key, so "is this key taken?" and "take
 *               it" are one indivisible step. Checking first and claiming second would leave a
 *               window in which two simultaneous arrivals both conclude they are first — precisely
 *               the case redundant network paths make common rather than theoretical.
 *
 *               In-memory, and therefore correct only in a single process. See the README: that is
 *               the assumption the whole design buys its simplicity with, and the one that must not
 *               be crossed quietly.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** A response held for replay. Immutable — every duplicate must see exactly the same bytes. */
data class StoredResponse(val status: Int, val contentType: String?, val body: ByteArray) {
    // Data classes compare ByteArray by identity, which would make two equal responses unequal.
    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is StoredResponse &&
                status == other.status &&
                contentType == other.contentType &&
                body.contentEquals(other.body))

    override fun hashCode(): Int =
        (status * 31 + (contentType?.hashCode() ?: 0)) * 31 + body.contentHashCode()
}

/** What a caller got from trying to claim a key. */
sealed interface Claim {
    /** You are the first: run the handler, then settle the slot. */
    data class Won(val key: String, val slot: CompletableFuture<StoredResponse>) : Claim

    /** Someone else is running it: await their answer rather than executing again. */
    data class Lost(val key: String, val slot: CompletableFuture<StoredResponse>) : Claim
}

/**
 * The four operations the filter needs, as an interface.
 *
 * The README names exactly one future that changes this component's shape: a second backend
 * instance, at which point the table has to move to shared storage. An interface is what keeps that
 * from being a breaking change — and, more immediately, it is what makes the
 * `@ConditionalOnMissingBean` on the store bean mean anything. A documented extension point that
 * cannot actually be extended is worse than none, because it is believed.
 */
interface IdempotencyStore {
    /** Take the key if it is free, otherwise hand back the slot its owner will settle. */
    fun claim(key: String): Claim

    /** Publish the answer and keep it for the rest of the TTL. */
    fun complete(key: String, response: StoredResponse)

    /** Publish the answer to whoever is waiting, but keep nothing. */
    fun completeAndRelease(key: String, response: StoredResponse)

    /**
     * Give up on the key: fail the waiters rather than let them block on an answer never coming.
     */
    fun abandon(key: String, cause: Throwable)
}

/**
 * The single-instance implementation. Correct only inside one process — see the README; that is the
 * assumption the whole design buys its simplicity with.
 */
class InMemoryIdempotencyStore(
    private val ttl: Duration,
    /** Injected so tests can advance time instead of sleeping through the TTL. */
    private val clock: () -> Long = System::currentTimeMillis,
) : IdempotencyStore {
    private class Entry(val slot: CompletableFuture<StoredResponse>, val createdAt: Long)

    private val entries = ConcurrentHashMap<String, Entry>()
    private val lastSweep = AtomicLong(0)

    override fun claim(key: String): Claim {
        sweepOccasionally()

        // The flag is set inside the remapping function, which the map runs under its per-key lock
        // and at most once per call. Whoever sees `true` is the single winner — decided by the map
        // itself rather than by any reasoning of ours. (`compute`, not `computeIfAbsent`, so that
        // an expired entry is replaced in the same atomic step instead of needing a second one.)
        var created = false
        val entry =
            entries.compute(key) { _, existing ->
                if (existing != null && !isExpired(existing)) {
                    existing
                } else {
                    created = true
                    Entry(CompletableFuture(), clock())
                }
            }!!

        return if (created) Claim.Won(key, entry.slot) else Claim.Lost(key, entry.slot)
    }

    /**
     * Publish the first answer to everyone waiting on this key, and keep it for later duplicates.
     */
    override fun complete(key: String, response: StoredResponse) {
        entries[key]?.slot?.complete(response)
    }

    /**
     * Hand the waiters this outcome but do not keep it.
     *
     * For a transient failure that is the honest answer twice over: the duplicates in flight are
     * told what actually happened, while the key is released so that a *later* retry genuinely
     * re-executes instead of being served a failure that has since stopped being true.
     */
    override fun completeAndRelease(key: String, response: StoredResponse) {
        val entry = entries.remove(key)
        entry?.slot?.complete(response)
    }

    /**
     * Abandon the key after an error that produced no response at all.
     *
     * Waiters are failed rather than left hanging until their timeout: a duplicate blocked on a
     * request that already died is waiting for something that will never arrive.
     */
    override fun abandon(key: String, cause: Throwable) {
        entries.remove(key)?.slot?.completeExceptionally(cause)
    }

    fun size(): Int = entries.size

    /**
     * Drop expired entries, at most every half-TTL.
     *
     * Piggy-backed on claims rather than run by a scheduler: the table only grows when writes
     * happen, so the moment a write happens is exactly when sweeping is worth its cost, and it
     * saves the library from owning a thread its host did not ask for.
     */
    private fun sweepOccasionally() {
        val now = clock()
        val previous = lastSweep.get()
        if (now - previous < ttl.toMillis() / 2) return
        if (!lastSweep.compareAndSet(previous, now)) return // another thread is already sweeping
        entries.entries.removeIf { isExpired(it.value) }
    }

    private fun isExpired(entry: Entry): Boolean = clock() - entry.createdAt > ttl.toMillis()
}
