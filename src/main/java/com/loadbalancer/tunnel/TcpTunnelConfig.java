package com.loadbalancer.tunnel;

import com.loadbalancer.config.LoadBalancerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Spring lifecycle manager for the {@link TcpTunnelServer}.
 *
 * <p>Automatically starts the L4 raw TCP server on application startup
 * (if {@code lb.tunnel-enabled} is true) and shuts it down on application termination.</p>
 */
@Component
public class TcpTunnelConfig implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TcpTunnelConfig.class);

    private final TcpTunnelServer tunnelServer;
    private final LoadBalancerProperties props;
    private boolean running = false;

    public TcpTunnelConfig(TcpTunnelServer tunnelServer, LoadBalancerProperties props) {
        this.tunnelServer = tunnelServer;
        this.props = props;
    }

    @Override
    public void start() {
        if (props.isTunnelEnabled()) {
            try {
                tunnelServer.start();
                running = true;
            } catch (Exception e) {
                log.error("Failed to bind L4 TCP Tunnel listener on port {}: {}", props.getTunnelPort(), e.getMessage());
            }
        } else {
            log.info("L4 TCP Tunnel is disabled (lb.tunnel-enabled=false)");
        }
    }

    @Override
    public void stop() {
        if (running) {
            tunnelServer.stop();
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
