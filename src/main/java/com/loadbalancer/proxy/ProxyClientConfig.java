package com.loadbalancer.proxy;

import com.loadbalancer.config.LoadBalancerProperties;
import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/**
 * Reactive non-blocking {@link WebClient} configuration.
 *
 * <p>Applies Netty connect and response timeouts from {@link LoadBalancerProperties}.
 * Runs entirely on Netty event loops without blocking worker threads.</p>
 */
@Configuration
public class ProxyClientConfig {

    @Bean
    public WebClient proxyWebClient(LoadBalancerProperties props) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) props.getConnectTimeoutMs())
                .responseTimeout(Duration.ofMillis(props.getResponseTimeoutMs()))
                .compress(true);

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
