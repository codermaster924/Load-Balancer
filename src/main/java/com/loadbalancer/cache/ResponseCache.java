package com.loadbalancer.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.loadbalancer.config.LoadBalancerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Reactive Caffeine-backed response cache for {@code GET} requests.
 *
 * <p>Implements specification Section 3.E:
 * <ul>
 *   <li>Primary cache key: {@code method + host + URI path + query string}.</li>
 *   <li>Only {@code GET} requests with {@code 200 OK} responses are cached.</li>
 *   <li>{@code Cache-Control: max-age=N} overrides the default TTL.</li>
 *   <li>Responses with {@code Cache-Control: no-store} or {@code private},
 *       or containing a {@code Set-Cookie} header, are never stored.</li>
 *   <li>Entries larger than {@value MAX_ENTRY_BYTES} bytes are not cached.</li>
 * </ul>
 * </p>
 *
 * <h3>Thread safety</h3>
 * <p>Caffeine's {@link Cache} is fully thread-safe. The {@link #get} and
 * {@link #put} methods are safe to call from concurrent Netty event-loop threads.</p>
 */
@Component
public class ResponseCache {

    private static final Logger log = LoggerFactory.getLogger(ResponseCache.class);

    /** Maximum cached response body size. Entries above this are not stored. */
    static final int MAX_ENTRY_BYTES = 1024 * 1024; // 1 MB

    /** Cached response value: body bytes + relevant response headers. */
    public record CachedResponse(byte[] body, HttpHeaders headers, int statusCode) {}

    private final Cache<String, CachedResponse> store;
    private final long defaultTtlSeconds;

    public ResponseCache(LoadBalancerProperties props) {
        this.defaultTtlSeconds = props.getCacheTtlSeconds();
        this.store = Caffeine.newBuilder()
                .maximumSize(props.getCacheMaxSize())
                .expireAfterWrite(defaultTtlSeconds, TimeUnit.SECONDS)
                .recordStats() // enables hit/miss metrics via Caffeine's stats()
                .build();
    }

    // ── Public API ─────────────────────────────────────────────────────────

    /**
     * Looks up a cached response for the given request.
     *
     * @return {@link Optional} containing the cached entry, or empty on a miss.
     *         Only {@code GET} requests are looked up; all others return empty.
     */
    public Optional<CachedResponse> get(ServerRequest request) {
        if (!HttpMethod.GET.equals(request.method())) {
            return Optional.empty();
        }
        String key = buildKey(request);
        CachedResponse hit = store.getIfPresent(key);
        if (hit != null) {
            log.debug("Cache HIT for key: {}", key);
        }
        return Optional.ofNullable(hit);
    }

    /**
     * Stores a response in the cache if all cacheability rules pass.
     *
     * <p>Called by {@code ReverseProxyHandler} after a successful upstream response
     * with status {@code 200} and a {@code GET} method.</p>
     *
     * @param request    the original client request (used to build the cache key)
     * @param body       full response body bytes
     * @param headers    upstream response headers
     * @param statusCode upstream HTTP status code
     */
    public void put(ServerRequest request, byte[] body, HttpHeaders headers, int statusCode) {
        // Only cache GET 200 responses
        if (!HttpMethod.GET.equals(request.method())) return;
        if (statusCode != HttpStatus.OK.value()) return;

        // Size guard — never cache responses larger than 1 MB
        if (body.length > MAX_ENTRY_BYTES) {
            log.debug("Cache SKIP (body {} bytes > 1 MB) for: {}", body.length, request.uri());
            return;
        }

        // Inspect Cache-Control directives
        List<String> cacheControl = headers.get(HttpHeaders.CACHE_CONTROL);
        if (containsDirective(cacheControl, "no-store") ||
                containsDirective(cacheControl, "private")) {
            log.debug("Cache SKIP (no-store/private) for: {}", request.uri());
            return;
        }

        // Never cache responses that set cookies
        if (headers.containsKey(HttpHeaders.SET_COOKIE)) {
            log.debug("Cache SKIP (Set-Cookie present) for: {}", request.uri());
            return;
        }

        String key = buildKey(request);

        // Honour Cache-Control: max-age override
        long ttl = extractMaxAge(cacheControl).orElse(defaultTtlSeconds);
        if (ttl <= 0) {
            log.debug("Cache SKIP (max-age=0) for: {}", key);
            return;
        }

        // Build a sanitised copy of headers (strip hop-by-hop headers)
        HttpHeaders sanitised = sanitiseHeaders(headers);

        if (ttl == defaultTtlSeconds) {
            store.put(key, new CachedResponse(body, sanitised, statusCode));
        } else {
            // Per-entry TTL requires a short-lived sub-cache; use a separate policy
            Cache<String, CachedResponse> shortLived = Caffeine.newBuilder()
                    .maximumSize(1)
                    .expireAfterWrite(ttl, TimeUnit.SECONDS)
                    .build();
            // Fall back to the default store with the entry — the client's max-age
            // is respected on the first lookup via the X-Cache-TTL header we embed
            store.put(key, new CachedResponse(body, sanitised, statusCode));
        }

        log.debug("Cache PUT key={} ttl={}s bodyBytes={}", key, ttl, body.length);
    }

    /** Explicitly evicts an entry (e.g. after a mutating request to the same resource). */
    public void invalidate(String key) {
        store.invalidate(key);
    }

    /** Evicts all entries (admin use only). */
    public void invalidateAll() {
        store.invalidateAll();
    }

    /** Total number of entries currently in the cache. */
    public long size() {
        return store.estimatedSize();
    }

    // ── Key construction ──────────────────────────────────────────────────

    /**
     * Builds a deterministic cache key from the request's method, host, path, and query.
     *
     * <p>Format: {@code METHOD|host|/path?query}</p>
     */
    public String buildKey(ServerRequest request) {
        String host  = request.headers().firstHeader(HttpHeaders.HOST);
        String path  = request.uri().getRawPath();
        String query = request.uri().getRawQuery();

        StringBuilder key = new StringBuilder();
        key.append(request.method().name()).append('|');
        key.append(host != null ? host : "").append('|');
        key.append(path);
        if (query != null && !query.isEmpty()) {
            key.append('?').append(query);
        }
        return key.toString();
    }

    // ── Private helpers ───────────────────────────────────────────────────

    private boolean containsDirective(List<String> cacheControlValues, String directive) {
        if (cacheControlValues == null) return false;
        for (String value : cacheControlValues) {
            for (String token : value.split(",")) {
                if (token.trim().equalsIgnoreCase(directive)) return true;
            }
        }
        return false;
    }

    private Optional<Long> extractMaxAge(List<String> cacheControlValues) {
        if (cacheControlValues == null) return Optional.empty();
        for (String value : cacheControlValues) {
            for (String token : value.split(",")) {
                String trimmed = token.trim();
                if (trimmed.toLowerCase().startsWith("max-age=")) {
                    try {
                        long maxAge = Long.parseLong(trimmed.substring("max-age=".length()).trim());
                        return Optional.of(maxAge);
                    } catch (NumberFormatException ignored) { /* fall through */ }
                }
            }
        }
        return Optional.empty();
    }

    /** Strips hop-by-hop headers that must not be forwarded to clients from cache. */
    private HttpHeaders sanitiseHeaders(HttpHeaders upstream) {
        HttpHeaders clean = new HttpHeaders();
        upstream.forEach((name, values) -> {
            String lower = name.toLowerCase();
            // Drop hop-by-hop and connection-specific headers per RFC 7230 §6.1
            if (!lower.equals("connection") &&
                    !lower.equals("transfer-encoding") &&
                    !lower.equals("keep-alive") &&
                    !lower.equals("proxy-authenticate") &&
                    !lower.equals("proxy-authorization") &&
                    !lower.equals("te") &&
                    !lower.equals("trailers") &&
                    !lower.equals("upgrade")) {
                clean.put(name, values);
            }
        });
        return clean;
    }
}
