package com.loadbalancer.proxy;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * Gateway route definitions for the Layer 7 Reverse Proxy.
 *
 * <p>Route 1 from specification:
 * <i>Path: {@code /**} (Catch-all for HTTP methods). {@code /lb/**} and {@code /actuator/**}
 * are reserved for the control plane and are never proxied.</i></p>
 */
@Configuration
public class ProxyRouterConfig {

    @Bean
    public RouterFunction<ServerResponse> proxyRoutes(ReverseProxyHandler proxyHandler) {
        return RouterFunctions.route(
                request -> {
                    String path = request.path();
                    return !path.startsWith("/lb/") &&
                            !path.equals("/lb") &&
                            !path.startsWith("/actuator");
                },
                proxyHandler::handle
        );
    }
}
