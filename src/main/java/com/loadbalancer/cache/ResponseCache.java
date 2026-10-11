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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Reactive Caffeine-backed response cache for {@code GET} requests.
 *
 * <p>Implements specification Section 2.B & 3.E:
 * <ul>
 *   <li>Primary cache key: {@code method + host + URI path + query string}.</li>
 *   <li>Two-level {@code Vary} lookup: records response {@code Vary} header names and incorporates
 *       request header values into the full cache key.</li>
 *   <li>Bypasses requests carrying an {@code Authorization} header.</li>
 *   <li>Only {@code GET} requests with {@code 200 OK} responses are cached.</li>
 *   <li>{@code Cache-Control: max-age=N} overrides the default TTL.</li>
 *   <li>Responses with {@code Cache-Control: no-store} or {@code private},
 *       or containing a {@code Set-Cookie} header, are never stored.</li>
 *   <li>Entries larger than {@value MAX_ENTRY_BYTES} bytes are not cached.</li>
 *   <li>Single-flight request coalescing: coalesces concurrent misses for the exact same key.</li>
 * </ul>
 * </p>
 *
 * <h3>Thread safety</h3>
 * <p>Caffeine's {@link Cache} and the underlying concurrency primitives are fully thread-safe.
 * The public methods are safe to call from concurrent Netty event-loop threads.</p>
 */
@Component
public class ResponseCache {

    private static final Logger log = LoggerFactory.getLogger(ResponseCache.class);

    /** Maximum cached response body size. Entries above this are not stored. */
    static final int MAX_ENTRY_BYTES = 1024 * 1024; // 1 MB

    /** Cached response value: body bytes + relevant response headers. */
    public record CachedResponse(byte[] body, HttpHeaders headers, int statusCode) {}

    private final Cache<String, CachedResponse> store;
    /** Two-level Vary index: primaryKey -> list of lowercase Vary header names. */
    private final Cache<String, List<String>> varyRules;
    /** Single-flight coalescing registry: fullKey -> in-flight Mono. */
    private final ConcurrentHashMap<String, Mono<CachedResponse>> inFlightFetches = new ConcurrentHashMap<>();

    private final long defaultTtlSeconds;

    public ResponseCache(LoadBalancerProperties props) {
        this.defaultTtlSeconds = props.getCacheTtlSeconds();
        this.store = Caffeine.newBuilder()
                .maximumSize(props.getCacheMaxSize())
                .expireAfterWrite(defaultTtlSeconds, TimeUnit.SECONDS)
                .recordStats() // enables hit/miss metrics via Caffeine's stats()
                .build();
        this.varyRules = Caffeine.newBuilder()
                .maximumSize(props.getCacheMaxSize())
                .expireAfterWrite(defaultTtlSeconds, TimeUnit.SECONDS)
                .build();
    }

    // ── Public API ─────────────────────────────────────────────────────────

    /**
     * Looks up a cached response for the given request.
     *
     * <p>Checks method (GET-only), skips requests with {@code Authorization},
     * and performs a two-level lookup taking {@code Vary} header rules into account.</p>
     *
     * @return {@link Optional} containing the cached entry, or empty on a miss.
     */
    public Optional<CachedResponse> get(ServerRequest request) {
        if (!HttpMethod.GET.equals(request.method())) {
            return Optional.empty();
        }
        // Specification Section 2.B: Do not cache requests carrying Authorization
        if (request.headers().firstHeader(HttpHeaders.AUTHORIZATION) != null) {
            log.debug("Cache SKIP (Authorization header present) for: {}", request.uri());
            return Optional.empty();
        }

        String key = getLookupKey(request);
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

        // Never cache requests carrying Authorization
        if (request.headers().firstHeader(HttpHeaders.AUTHORIZATION) != null) {
            log.debug("Cache SKIP (Authorization header present) for: {}", request.uri());
            return;
        }

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

        // Handle Vary: * (cannot be cached per RFC 7234)
        List<String> varyHeaders = headers.get(HttpHeaders.VARY);
        if (varyHeaders != null) {
            for (String v : varyHeaders) {
                if (v.contains("*")) {
                    log.debug("Cache SKIP (Vary: *) for: {}", request.uri());
                    return;
                }
            }
        }

        String primaryKey = buildPrimaryKey(request);

        // Record Vary rules if present
        List<String> varyTokens = extractVaryHeaderTokens(varyHeaders);
        if (!varyTokens.isEmpty()) {
            varyRules.put(primaryKey, varyTokens);
        }

        String fullKey = buildFullKey(primaryKey, request, varyTokens);

        // Honour Cache-Control: max-age override
        long ttl = extractMaxAge(cacheControl).orElse(defaultTtlSeconds);
        if (ttl <= 0) {
            log.debug("Cache SKIP (max-age=0) for: {}", fullKey);
            return;
        }

        // Build a sanitised copy of headers (strip hop-by-hop headers)
        HttpHeaders sanitised = sanitiseHeaders(headers);
        store.put(fullKey, new CachedResponse(body, sanitised, statusCode));

        log.debug("Cache PUT key={} ttl={}s bodyBytes={}", fullKey, ttl, body.length);
    }

    /**
     * Coalesces concurrent cache misses for the exact same request key into a single upstream fetch.
     *
     * <p>If multiple concurrent calls arrive for the same resource before it is cached,
     * only one fetch supplier is executed; all callers receive the shared Mono result.</p>
     *
     * @param request       the incoming client request
     * @param fetchSupplier supplier of the upstream fetch Mono
     * @return Mono yielding the cached or fetched response
     */
    public Mono<CachedResponse> coalesce(ServerRequest request, Supplier<Mono<CachedResponse>> fetchSupplier) {
        String key = getLookupKey(request);
        return inFlightFetches.computeIfAbsent(key, k ->
                Mono.defer(fetchSupplier)
                        .doFinally(signal -> inFlightFetches.remove(k))
                        .cache()
        );
    }

    /** Explicitly evicts an entry (e.g. after a mutating request to the same resource). */
    public void invalidate(String key) {
        store.invalidate(key);
        varyRules.invalidate(key);
    }

    /** Evicts all entries (admin use only). */
    public void invalidateAll() {
        store.invalidateAll();
        varyRules.invalidateAll();
        inFlightFetches.clear();
    }

    /** Total number of entries currently in the cache. */
    public long size() {
        return store.estimatedSize();
    }

    // ── Key construction ──────────────────────────────────────────────────

    /**
     * Resolves the full cache key for a request, evaluating any existing {@code Vary} rules.
     */
    public String getLookupKey(ServerRequest request) {
        String primaryKey = buildPrimaryKey(request);
        List<String> varyHeaders = varyRules.getIfPresent(primaryKey);
        return buildFullKey(primaryKey, request, varyHeaders);
    }

    /**
     * Builds the primary cache key: {@code METHOD|host|/path?query}.
     */
    public String buildPrimaryKey(ServerRequest request) {
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

    /**
     * Builds the full cache key by appending sorted Vary header values to the primary key.
     */
    public String buildFullKey(String primaryKey, ServerRequest request, List<String> varyTokens) {
        if (varyTokens == null || varyTokens.isEmpty()) {
            return primaryKey;
        }

        StringBuilder fullKey = new StringBuilder(primaryKey);
        // Sort tokens for deterministic key generation
        List<String> sorted = new ArrayList<>(varyTokens);
        Collections.sort(sorted);

        for (String headerName : sorted) {
            String val = request.headers().firstHeader(headerName);
            fullKey.append("|vary:").append(headerName).append('=').append(val != null ? val : "");
        }
        return fullKey.toString();
    }

    /**
     * Backward-compatible key builder.
     */
    public String buildKey(ServerRequest request) {
        return getLookupKey(request);
    }

    // ── Private helpers ───────────────────────────────────────────────────

    private List<String> extractVaryHeaderTokens(List<String> varyHeaderValues) {
        if (varyHeaderValues == null || varyHeaderValues.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> tokens = new HashSet<>();
        for (String value : varyHeaderValues) {
            for (String part : value.split(",")) {
                String trimmed = part.trim().toLowerCase();
                if (!trimmed.isEmpty() && !trimmed.equals("*")) {
                    tokens.add(trimmed);
                }
            }
        }
        return new ArrayList<>(tokens);
    }

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
