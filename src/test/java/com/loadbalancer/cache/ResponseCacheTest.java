package com.loadbalancer.cache;

import com.loadbalancer.config.LoadBalancerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.reactive.function.server.MockServerRequest;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ResponseCacheTest {

    private ResponseCache cache;
    private LoadBalancerProperties props;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCacheMaxSize(1000);
        props.setCacheTtlSeconds(60);
        cache = new ResponseCache(props);
    }

    @Test
    @DisplayName("bypasses caching when Authorization header is present in request")
    void bypassesCachingWhenAuthorizationHeaderPresent() {
        ServerRequest reqWithAuth = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(URI.create("http://localhost:8080/secure/data"))
                .header(HttpHeaders.AUTHORIZATION, "Bearer my-secret-token")
                .build();

        HttpHeaders upstreamHeaders = new HttpHeaders();
        upstreamHeaders.add("Content-Type", "application/json");

        cache.put(reqWithAuth, "{\"secret\":\"value\"}".getBytes(), upstreamHeaders, 200);

        // Put should be ignored
        Optional<ResponseCache.CachedResponse> cached = cache.get(reqWithAuth);
        assertTrue(cached.isEmpty(), "Requests with Authorization must not be cached");
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("partitions cache entries based on two-level Vary header lookup")
    void partitionsCacheEntriesByVaryHeader() {
        URI targetUri = URI.create("http://example.com/api/content");

        ServerRequest reqGzip = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(targetUri)
                .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
                .build();

        ServerRequest reqBr = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(targetUri)
                .header(HttpHeaders.ACCEPT_ENCODING, "br")
                .build();

        HttpHeaders headersVary = new HttpHeaders();
        headersVary.add(HttpHeaders.VARY, "Accept-Encoding");
        headersVary.add(HttpHeaders.CONTENT_TYPE, "text/plain");

        // Cache gzip response
        cache.put(reqGzip, "gzipped-payload".getBytes(), headersVary, 200);

        // Lookup with Accept-Encoding: gzip should hit
        Optional<ResponseCache.CachedResponse> hitGzip = cache.get(reqGzip);
        assertTrue(hitGzip.isPresent(), "Request with matching Vary header value must hit cache");
        assertEquals("gzipped-payload", new String(hitGzip.get().body()));

        // Lookup with Accept-Encoding: br must MISS because it has a different Vary value
        Optional<ResponseCache.CachedResponse> missBr = cache.get(reqBr);
        assertTrue(missBr.isEmpty(), "Request with differing Vary header value must miss cache");

        // Now store br response
        cache.put(reqBr, "brotli-payload".getBytes(), headersVary, 200);

        // Now both should hit their respective cached versions
        assertEquals("gzipped-payload", new String(cache.get(reqGzip).get().body()));
        assertEquals("brotli-payload", new String(cache.get(reqBr).get().body()));
        assertEquals(2, cache.size());
    }

    @Test
    @DisplayName("skips caching when response contains Vary: *")
    void skipsCachingWhenVaryStarPresent() {
        ServerRequest req = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(URI.create("http://example.com/wildcard"))
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.VARY, "*");

        cache.put(req, "never-cache-me".getBytes(), headers, 200);

        assertTrue(cache.get(req).isEmpty(), "Vary: * responses must never be cached");
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("coalesces concurrent requests for the same key into a single upstream fetch")
    void coalescesConcurrentRequestsIntoSingleFlight() throws Exception {
        ServerRequest req = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(URI.create("http://example.com/single-flight"))
                .build();

        int concurrency = 8;
        AtomicInteger fetchCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);

        List<ResponseCache.CachedResponse> results = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    ResponseCache.CachedResponse resp = cache.coalesce(req, () -> {
                        fetchCount.incrementAndGet();
                        // Simulate network delay
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ignored) {}
                        return Mono.just(new ResponseCache.CachedResponse(
                                "shared-data".getBytes(), new HttpHeaders(), 200));
                    }).block();
                    synchronized (results) {
                        results.add(resp);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Release all threads simultaneously
        startLatch.countDown();
        doneLatch.await();
        pool.shutdown();

        assertEquals(concurrency, results.size(), "All concurrent callers must receive a response");
        assertEquals(1, fetchCount.get(), "Upstream fetch supplier must be invoked exactly once for coalesced requests");
        for (ResponseCache.CachedResponse res : results) {
            assertEquals("shared-data", new String(res.body()));
        }
    }

    @Test
    @DisplayName("skips responses containing Set-Cookie or no-store or private")
    void skipsUncacheableCacheControlAndCookies() {
        ServerRequest req = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(URI.create("http://example.com/nocache"))
                .build();

        // 1. Set-Cookie
        HttpHeaders cookieHeaders = new HttpHeaders();
        cookieHeaders.add(HttpHeaders.SET_COOKIE, "session=abc");
        cache.put(req, "data".getBytes(), cookieHeaders, 200);
        assertTrue(cache.get(req).isEmpty());

        // 2. Cache-Control: no-store
        HttpHeaders noStoreHeaders = new HttpHeaders();
        noStoreHeaders.add(HttpHeaders.CACHE_CONTROL, "no-store");
        cache.put(req, "data".getBytes(), noStoreHeaders, 200);
        assertTrue(cache.get(req).isEmpty());

        // 3. Cache-Control: private
        HttpHeaders privateHeaders = new HttpHeaders();
        privateHeaders.add(HttpHeaders.CACHE_CONTROL, "private, max-age=300");
        cache.put(req, "data".getBytes(), privateHeaders, 200);
        assertTrue(cache.get(req).isEmpty());
    }

    @Test
    @DisplayName("skips bodies larger than 1 MB")
    void skipsOversizedBodies() {
        ServerRequest req = MockServerRequest.builder()
                .method(HttpMethod.GET)
                .uri(URI.create("http://example.com/large"))
                .build();

        byte[] largeBody = new byte[ResponseCache.MAX_ENTRY_BYTES + 10];
        cache.put(req, largeBody, new HttpHeaders(), 200);

        assertTrue(cache.get(req).isEmpty(), "Bodies over 1 MB must not be cached");
        assertEquals(0, cache.size());
    }
}
