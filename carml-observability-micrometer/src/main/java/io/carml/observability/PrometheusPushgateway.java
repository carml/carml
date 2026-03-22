package io.carml.observability;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/**
 * Pushes Prometheus metrics from a {@link PrometheusMeterRegistry} to a
 * <a href="https://github.com/prometheus/pushgateway">Prometheus Pushgateway</a>.
 *
 * <p>Designed for batch mapping runs (CLI, benchmarks, KROWN) where CARML is not a long-running
 * server. After the mapping completes, call {@link #push} to send all accumulated metrics to the
 * pushgateway, where Prometheus scrapes them and Grafana visualizes them.
 *
 * <p>Usage:
 * <pre>{@code
 * var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
 * var observer = MetricsObserver.create(registry);
 *
 * var mapper = RdfRmlMapper.builder()
 *     .observer(observer)
 *     .build();
 *
 * mapper.map().count().block();
 *
 * PrometheusPushgateway.push(registry, "localhost:9091", "carml_benchmark", Map.of(
 *     "evaluator", "duckdb",
 *     "dataset", "krown-student-10k"
 * ));
 * }</pre>
 */
@Slf4j
public final class PrometheusPushgateway {

    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private PrometheusPushgateway() {}

    /**
     * Pushes all metrics from the registry to the Pushgateway.
     *
     * @param registry the Prometheus meter registry containing the metrics
     * @param pushgatewayHost the pushgateway host:port (e.g., {@code "localhost:9091"})
     * @param jobName the Prometheus job label for this push (e.g., {@code "carml_benchmark"})
     * @param groupingLabels additional labels to add to the push (e.g., evaluator, dataset)
     */
    public static void push(
            PrometheusMeterRegistry registry,
            String pushgatewayHost,
            String jobName,
            java.util.Map<String, String> groupingLabels) {
        var pathBuilder = new StringBuilder("/metrics/job/").append(jobName);
        groupingLabels.forEach(
                (key, value) -> pathBuilder.append('/').append(key).append('/').append(value));

        var scheme = pushgatewayHost.startsWith("https://") || pushgatewayHost.startsWith("http://") ? "" : "http://";
        var uri = URI.create("%s%s%s".formatted(scheme, pushgatewayHost, pathBuilder));
        var body = registry.scrape();

        var request = HttpRequest.newBuilder()
                .uri(uri)
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        try {
            var response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                LOG.info("Pushed {} bytes of metrics to {}", body.length(), uri);
            } else {
                LOG.warn("Pushgateway returned HTTP {}: {}", response.statusCode(), response.body());
            }
        } catch (IOException e) {
            LOG.warn("Failed to push metrics to {}: {}", uri, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while pushing metrics to {}", uri);
        }
    }

    /**
     * Pushes metrics with no additional grouping labels.
     */
    public static void push(PrometheusMeterRegistry registry, String pushgatewayHost, String jobName) {
        push(registry, pushgatewayHost, jobName, java.util.Map.of());
    }
}
