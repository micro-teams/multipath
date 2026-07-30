/*
 *  Description: The cross-cutting layer that absorbs duplicate writes.
 *
 *               It wraps the handler rather than preceding it, and the distinction matters: the key
 *               is claimed *before* the controller, but the answer can only be stored *after* it.
 *               So the winner passes straight through and settles the slot on its way out, while
 *               every loser is stopped in front of the controller and served the winner's answer.
 *
 *               A loser is replayed, never rejected. Behind a duplicate is a real client really
 *               waiting: telling it "duplicate" would report a failure for a write that succeeded,
 *               and it would then quite reasonably retry — turning de-duplication into a machine
 *               for generating more duplicates.
 *
 *               Nothing here is visible to a controller, which is the point. Redundant paths are a
 *               transport concern, and a business method that had to know about them would have to
 *               be re-examined every time one was added.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingResponseWrapper

class IdempotencyFilter(
    private val properties: IdempotencyProperties,
    private val store: IdempotencyStore,
) : OncePerRequestFilter() {

    companion object {
        /** Stamped on a replayed answer, so a client (and a packet capture) can tell them apart. */
        const val REPLAY_HEADER = "Idempotency-Replayed"

        /** Carries the in-progress claim and its wrapper across an async dispatch. */
        private val CLAIM_ATTRIBUTE = IdempotencyFilter::class.java.name + ".claim"
        private val WRAPPER_ATTRIBUTE = IdempotencyFilter::class.java.name + ".wrapper"
    }

    /**
     * Run on the async dispatch too, not just the initial one.
     *
     * A controller returning `CompletableFuture` or `DeferredResult` hands control back to the
     * container long before it has written anything. Left at the default, this filter would settle
     * the slot and flush the wrapper while the body was still unwritten — the caller would get an
     * empty 200 and every duplicate would be replayed that emptiness. Correctness for those
     * endpoints requires being present when the second dispatch finishes.
     */
    override fun shouldNotFilterAsyncDispatch(): Boolean = false

    /**
     * Requests without a key, and reads, are not this filter's business — and `shouldNotFilter`
     * rather than an early return inside the body, so an unguarded request never even pays for the
     * response wrapper.
     */
    override fun shouldNotFilter(request: HttpServletRequest): Boolean =
        request.getHeader(properties.header).isNullOrBlank() ||
            request.method.uppercase() !in properties.methods

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        chain: FilterChain,
    ) {
        // The async dispatch is the second half of a request this filter already claimed a key for.
        // Pick the claim back up rather than treating it as a new arrival, which would deadlock the
        // request against its own slot.
        val resumed = request.getAttribute(CLAIM_ATTRIBUTE) as Claim.Won?
        if (resumed != null) {
            val wrapper = request.getAttribute(WRAPPER_ATTRIBUTE) as ContentCachingResponseWrapper
            finish(resumed, request, wrapper, chain)
            return
        }

        // Scope the key by method and path. A client is responsible for its keys being unique, but
        // one accidentally reused across two different endpoints should collide with itself, not
        // serve one endpoint's answer to the other.
        val key = "${request.method}:${request.requestURI}:${request.getHeader(properties.header)}"

        when (val claim = store.claim(key)) {
            is Claim.Won -> execute(claim, request, response, chain)
            is Claim.Lost -> replay(claim, response)
        }
    }

    private fun execute(
        claim: Claim.Won,
        request: HttpServletRequest,
        response: HttpServletResponse,
        chain: FilterChain,
    ) {
        finish(claim, request, ContentCachingResponseWrapper(response), chain)
    }

    /**
     * Run the handler and settle the slot — on whichever dispatch actually produces the response.
     *
     * For an ordinary controller that is this dispatch. For an async one, `chain.doFilter` returns
     * as soon as the handler has promised an answer rather than produced one, and settling then
     * would store an empty body and flush it to the caller. So in that case everything is handed to
     * the async dispatch, which arrives here again once the answer is real.
     */
    private fun finish(
        claim: Claim.Won,
        request: HttpServletRequest,
        caching: ContentCachingResponseWrapper,
        chain: FilterChain,
    ) {
        var settled = false
        try {
            chain.doFilter(request, caching)

            if (request.isAsyncStarted) {
                // Not finished — only promised. Carry the claim over and leave the slot open; the
                // duplicates waiting on it are waiting for the real answer, and they should.
                request.setAttribute(CLAIM_ATTRIBUTE, claim)
                request.setAttribute(WRAPPER_ATTRIBUTE, caching)
                return
            }

            val stored =
                StoredResponse(
                    status = caching.status,
                    contentType = caching.contentType,
                    body = caching.contentAsByteArray,
                )

            // The status is not consulted, on purpose (nictheboy, 2026-07-30). A key identifies one
            // attempt, and an attempt's outcome is whatever the server said — 200 and 500 alike.
            // Deciding that some failures "deserve" a re-run would be retry policy, and retry
            // policy belongs to whoever mints keys: a business-level retry is a new logical write
            // with a new key and really executes, while the same key reappearing is the transport
            // retrying one attempt, which must not become a second execution. Size is the only
            // reason to decline, and that is a question about heap, not about meaning.
            if (stored.body.size <= properties.maxResponseBytes) {
                store.complete(claim.key, stored)
            } else {
                store.completeAndRelease(claim.key, stored)
            }
            settled = true
        } catch (e: Throwable) {
            // The handler blew up without producing a response anyone can replay. Fail the waiters
            // now rather than leaving them to time out on an answer that is never coming.
            store.abandon(claim.key, e)
            settled = true
            throw e
        } finally {
            if (!settled && !request.isAsyncStarted) {
                store.abandon(claim.key, IllegalStateException("request did not settle"))
            }
            // Must happen on every path that really ended: the real response is still empty until
            // the wrapper's buffer is copied into it. Not while async is in flight, though — there
            // is nothing to copy yet, and copying now is precisely how the body gets lost.
            if (!request.isAsyncStarted) caching.copyBodyToResponse()
        }
    }

    private fun replay(claim: Claim.Lost, response: HttpServletResponse) {
        val stored =
            try {
                await(claim.slot)
            } catch (_: TimeoutException) {
                // Never fall through to the handler. "The first one is slow" is not a reason to
                // execute the write a second time; being told to retry is strictly better than
                // being charged twice.
                writeProblem(
                    response,
                    HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "the original request for this idempotency key is still in flight",
                )
                return
            } catch (e: ExecutionException) {
                writeProblem(
                    response,
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "the original request for this idempotency key failed: ${e.cause?.message}",
                )
                return
            }

        response.status = stored.status
        stored.contentType?.let { response.contentType = it }
        response.setHeader(REPLAY_HEADER, "true")
        response.outputStream.write(stored.body)
        response.outputStream.flush()
    }

    private fun await(slot: CompletableFuture<StoredResponse>): StoredResponse =
        slot.get(properties.wait.toMillis(), TimeUnit.MILLISECONDS)

    private fun writeProblem(response: HttpServletResponse, status: Int, message: String) {
        response.status = status
        response.contentType = "application/json"
        response.writer.write("""{"error":${quote(message)}}""")
    }

    private fun quote(value: String): String =
        "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
