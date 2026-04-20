package io.carml.observability;

import io.carml.engine.CheckpointInfo;
import io.carml.engine.MappingError;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingExecutionResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Statement;

/**
 * A {@link MappingExecutionObserver} that publishes mapping execution metrics to a Micrometer
 * {@link MeterRegistry}. Supports any Micrometer-compatible backend (Prometheus, Grafana,
 * Datadog, InfluxDB, etc.).
 *
 * <p>Published metrics:
 * <table>
 *   <tr><th>Metric</th><th>Type</th><th>Tags</th><th>Description</th></tr>
 *   <tr><td>{@code carml.statements.generated}</td><td>Counter</td><td>{@code triples_map}</td>
 *       <td>Total statements produced per TriplesMap</td></tr>
 *   <tr><td>{@code carml.iterations.processed}</td><td>Counter</td><td>{@code triples_map}, {@code evaluator}</td>
 *       <td>ViewIterations processed per TriplesMap</td></tr>
 *   <tr><td>{@code carml.iterations.deduplicated}</td><td>Counter</td><td>{@code triples_map}</td>
 *       <td>ViewIterations eliminated by dedup</td></tr>
 *   <tr><td>{@code carml.errors}</td><td>Counter</td><td>{@code triples_map}</td>
 *       <td>Errors per TriplesMap</td></tr>
 *   <tr><td>{@code carml.mapping.duration}</td><td>Timer</td><td>{@code triples_map}</td>
 *       <td>Execution time per TriplesMap</td></tr>
 *   <tr><td>{@code carml.view.evaluation.duration}</td><td>Timer</td><td>{@code view}, {@code evaluator}</td>
 *       <td>View evaluation time per view and evaluator</td></tr>
 *   <tr><td>{@code carml.statements.total}</td><td>Counter</td><td>—</td>
 *       <td>Total statements generated across all mappings</td></tr>
 *   <tr><td>{@code carml.mapping.completed}</td><td>Counter</td><td>{@code triples_map}, {@code reason}</td>
 *       <td>Completed mapping executions by reason (EXHAUSTED, CANCELLED, ERROR)</td></tr>
 *   <tr><td>{@code carml.mapping.statements}</td><td>DistributionSummary</td><td>{@code triples_map}</td>
 *       <td>Statements produced per mapping execution (count/total/max/mean)</td></tr>
 *   <tr><td>{@code carml.view.evaluation.iterations}</td><td>DistributionSummary</td>
 *       <td>{@code view}, {@code evaluator}</td>
 *       <td>Iterations produced per view evaluation (count/total/max/mean)</td></tr>
 *   <tr><td>{@code carml.mappings.active}</td><td>Gauge</td><td>—</td>
 *       <td>Number of TriplesMap evaluations currently in progress</td></tr>
 * </table>
 *
 * <p>Usage:
 * <pre>{@code
 * var mapper = RdfRmlMapper.builder()
 *     .observer(MetricsObserver.create(meterRegistry))
 *     .build();
 * }</pre>
 *
 * <p>This observer is thread-safe and accumulates no unbounded state. Evaluator names are tracked
 * per active mapping via identity-keyed map and cleaned up on mapping completion.
 */
@Slf4j
public final class MetricsObserver implements MappingExecutionObserver {

    private static final String TAG_TRIPLES_MAP = "triples_map";
    private static final String TAG_EVALUATOR = "evaluator";
    private static final String TAG_VIEW = "view";
    private static final String UNKNOWN = "unknown";

    private final MeterRegistry registry;

    private final AtomicLong activeMappings = new AtomicLong(0);

    /**
     * Tracks the evaluator class name per active mapping. Keyed by {@link ResolvedMapping} identity
     * (not name string) to avoid collisions between mappings with the same resource name. Entries
     * are added in {@link #onViewEvaluationStart} and removed in {@link #onMappingComplete}.
     */
    private final ConcurrentMap<ResolvedMapping, String> evaluatorPerMapping = new ConcurrentHashMap<>();

    private MetricsObserver(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        Gauge.builder("carml.mappings.active", this, obs -> obs.activeMappings.get())
                .description("Number of TriplesMap evaluations currently in progress")
                .register(registry);
    }

    /**
     * Creates a new {@link MetricsObserver} publishing to the given registry.
     *
     * @param registry the Micrometer meter registry
     * @return a new metrics observer
     */
    public static MetricsObserver create(MeterRegistry registry) {
        return new MetricsObserver(registry);
    }

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        activeMappings.incrementAndGet();
    }

    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        activeMappings.decrementAndGet();

        var triplesMapName = mappingName(mapping);

        Timer.builder("carml.mapping.duration")
                .description("Execution time per TriplesMap")
                .tag(TAG_TRIPLES_MAP, triplesMapName)
                .register(registry)
                .record(result.duration());

        // Statement and dedup counts from the authoritative result totals — not per-callback —
        // to avoid double-counting and reduce hot-path overhead.
        counter("carml.statements.generated", "Total RDF statements generated per TriplesMap", triplesMapName)
                .increment(result.statementsGenerated());
        counter(
                        "carml.iterations.deduplicated",
                        "ViewIterations eliminated by deduplication per TriplesMap",
                        triplesMapName)
                .increment(result.iterationsDeduplicated());

        Counter.builder("carml.statements.total")
                .description("Total RDF statements generated across all mappings")
                .register(registry)
                .increment(result.statementsGenerated());

        Counter.builder("carml.mapping.completed")
                .description("Completed mapping executions by TriplesMap and completion reason")
                .tag(TAG_TRIPLES_MAP, triplesMapName)
                .tag("reason", result.completionReason().name())
                .register(registry)
                .increment();

        DistributionSummary.builder("carml.mapping.statements")
                .description("Statements produced per mapping execution")
                .tag(TAG_TRIPLES_MAP, triplesMapName)
                .register(registry)
                .record(result.statementsGenerated());

        evaluatorPerMapping.remove(mapping);
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        // Checkpoint is a no-op for counters. Statement and dedup counts are recorded exclusively
        // in onMappingComplete from the authoritative result totals. Incrementing here would
        // double-count because MappingExecutionResult totals are cumulative.
        // Real-time streaming progress is visible via carml.iterations.processed (per-callback)
        // and carml.mappings.active (gauge).
    }

    @Override
    public void onViewEvaluationStart(ResolvedMapping mapping, LogicalViewEvaluator evaluator) {
        evaluatorPerMapping.put(mapping, evaluator.getClass().getSimpleName());
    }

    @Override
    public void onViewIteration(ResolvedMapping mapping, ViewIteration iteration) {
        var triplesMapName = mappingName(mapping);
        var evaluatorName = evaluatorPerMapping.getOrDefault(mapping, UNKNOWN);

        // Iteration counts are recorded per-callback (not from result totals) because iteration
        // processing is the primary indicator of pipeline progress during execution.
        Counter.builder("carml.iterations.processed")
                .description("ViewIterations processed per TriplesMap")
                .tag(TAG_TRIPLES_MAP, triplesMapName)
                .tag(TAG_EVALUATOR, evaluatorName)
                .register(registry)
                .increment();
    }

    @Override
    public void onViewEvaluationComplete(ResolvedMapping mapping, long iterationCount, Duration duration) {
        var evaluatorName = evaluatorPerMapping.getOrDefault(mapping, UNKNOWN);
        var viewName = mapping.getEffectiveView().getResourceName();

        var resolvedViewName = viewName != null ? viewName : UNKNOWN;

        Timer.builder("carml.view.evaluation.duration")
                .description("View evaluation time per view and evaluator")
                .tag(TAG_VIEW, resolvedViewName)
                .tag(TAG_EVALUATOR, evaluatorName)
                .register(registry)
                .record(duration);

        DistributionSummary.builder("carml.view.evaluation.iterations")
                .description("Iterations produced per view evaluation")
                .tag(TAG_VIEW, resolvedViewName)
                .tag(TAG_EVALUATOR, evaluatorName)
                .register(registry)
                .record(iterationCount);
    }

    @Override
    public void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, Set<LogicalTarget> logicalTargets) {
        // Statement counts are recorded in onMappingComplete/onCheckpoint from authoritative totals.
        // Per-statement callback is intentionally a no-op to reduce hot-path overhead.
    }

    @Override
    public void onIterationDeduplicated(ResolvedMapping mapping, ViewIteration iteration) {
        // Dedup counts are recorded in onMappingComplete from the authoritative result totals.
    }

    @Override
    public void onError(ResolvedMapping mapping, ViewIteration iteration, MappingError error) {
        Counter.builder("carml.errors")
                .description("Errors encountered per TriplesMap")
                .tag(TAG_TRIPLES_MAP, mappingName(mapping))
                .register(registry)
                .increment();
    }

    private Counter counter(String name, String description, String triplesMapName) {
        return Counter.builder(name)
                .description(description)
                .tag(TAG_TRIPLES_MAP, triplesMapName)
                .register(registry);
    }

    private static String mappingName(ResolvedMapping mapping) {
        var name = mapping.getOriginalTriplesMap().getResourceName();
        return name != null ? name : UNKNOWN;
    }
}
