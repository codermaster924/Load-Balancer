package com.loadbalancer.control;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * WebFlux router for the Load Balancer control plane.
 *
 * <p>Maps the three management routes from specification Section 2.B to
 * {@link ControlPlaneHandler}. These paths are explicitly excluded from the
 * catch-all proxy route in {@code ProxyRouterConfig}.</p>
 *
 * <ul>
 *   <li>{@code GET    /lb/metrics}        — fleet status snapshot</li>
 *   <li>{@code POST   /lb/backends}       — register a backend node</li>
 *   <li>{@code DELETE /lb/backends/{id}}  — graceful drain and remove</li>
 * </ul>
 */
@Configuration
public class ControlPlaneRouterConfig {

    @Bean
    public RouterFunction<ServerResponse> controlPlaneRoutes(ControlPlaneHandler handler) {
        return RouterFunctions.route()
                // Route 3: Fleet observability
                .GET("/lb/metrics",
                        RequestPredicates.accept(MediaType.ALL),
                        handler::getMetrics)

                // Route 4: Dynamic backend registration
                .POST("/lb/backends",
                        RequestPredicates.contentType(MediaType.APPLICATION_JSON),
                        handler::registerBackend)

                // Route 5: Graceful backend removal
                .DELETE("/lb/backends/{id}",
                        handler::removeBackend)

                .build();
    }
}
