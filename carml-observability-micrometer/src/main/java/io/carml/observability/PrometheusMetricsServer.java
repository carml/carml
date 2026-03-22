package io.carml.observability;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

/**
 * A lightweight HTTP server that exposes Prometheus metrics for scraping. Uses the JDK's built-in
 * {@link HttpServer} — no additional dependencies required.
 *
 * <p>Starts on the given port and serves the Prometheus text format at {@code /metrics}. Prometheus
 * scrapes this endpoint at its configured interval (e.g., every 5s), providing real-time
 * time-series data for throughput graphs, iteration progress, and duration tracking.
 *
 * <p>Usage:
 * <pre>{@code
 * var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
 * var server = PrometheusMetricsServer.start(registry, 9092);
 * // ... run mapping ...
 * server.stop();
 * }</pre>
 */
@Slf4j
public final class PrometheusMetricsServer {

    private final HttpServer httpServer;

    private PrometheusMetricsServer(HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    /**
     * Starts a metrics server on the given port.
     *
     * @param registry the Prometheus meter registry to serve
     * @param port the port to bind to
     * @return the running server instance
     * @throws IOException if the server cannot bind to the port
     */
    public static PrometheusMetricsServer start(PrometheusMeterRegistry registry, int port) throws IOException {
        var server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            var responseBytes = registry.scrape().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (var os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        });
        server.setExecutor(null);
        server.start();
        LOG.info("Prometheus metrics server started on http://localhost:{}/metrics", port);
        return new PrometheusMetricsServer(server);
    }

    /**
     * Stops the metrics server.
     */
    public void stop() {
        httpServer.stop(0);
        LOG.info("Prometheus metrics server stopped");
    }
}
