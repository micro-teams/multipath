/*
 *  Description: Wiring. Adding the dependency is the entire installation — there is no second step,
 *               no annotation to add to a controller, and nothing to inject.
 *
 *  Author(s):
 *      agent4
 */

package app.microteams.multipath.idempotency

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.core.Ordered

@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "multipath.idempotency", name = ["enabled"], matchIfMissing = true)
@EnableConfigurationProperties(IdempotencyProperties::class)
class IdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun idempotencyStore(properties: IdempotencyProperties): IdempotencyStore =
        InMemoryIdempotencyStore(properties.ttl)

    /**
     * Registered near the end of the filter chain, after authentication.
     *
     * Order matters and the reason is not cosmetic: claiming a key before the caller has been
     * authenticated would let an unauthenticated request burn a key — and, worse, let one caller be
     * served another's cached answer by guessing a key.
     */
    @Bean
    @ConditionalOnMissingBean(IdempotencyFilter::class)
    fun idempotencyFilterRegistration(
        properties: IdempotencyProperties,
        store: IdempotencyStore,
    ): FilterRegistrationBean<IdempotencyFilter> =
        FilterRegistrationBean(IdempotencyFilter(properties, store)).apply {
            order = Ordered.LOWEST_PRECEDENCE - 100
        }
}
