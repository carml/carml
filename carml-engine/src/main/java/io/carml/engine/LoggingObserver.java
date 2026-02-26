package io.carml.engine;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.TermMap;
import java.time.Duration;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Statement;

/**
 * A {@link MappingExecutionObserver} implementation that logs mapping execution events using
 * structured SLF4J logging at appropriate levels.
 *
 * <p>Logging levels:
 * <ul>
 *   <li>{@code INFO} -- mapping start and complete with summary statistics</li>
 *   <li>{@code DEBUG} -- view evaluation start/complete, checkpoint progress</li>
 *   <li>{@code TRACE} -- individual iterations, statements, and deduplication events</li>
 *   <li>{@code WARN} -- errors encountered during mapping execution</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * var mapper = RdfRmlMapper.builder()
 *     .observer(LoggingObserver.create())
 *     .build();
 * }</pre>
 *
 * <p>This observer accumulates no state and is safe for use with unbounded streams.
 */
@Slf4j
public final class LoggingObserver implements MappingExecutionObserver {

    private LoggingObserver() {}

    /**
     * Creates a new {@link LoggingObserver} instance.
     *
     * @return a new logging observer
     */
    public static LoggingObserver create() {
        return new LoggingObserver();
    }

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Mapping {} started (view: {})",
                    mapping.getOriginalTriplesMap().getResourceName(),
                    mapping.getEffectiveView().getResourceName());
        }
    }

    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Mapping {} completed: {} statements, {} iterations, {} deduplicated, {} errors ({}, reason: {})",
                    mapping.getOriginalTriplesMap().getResourceName(),
                    formatNumber(result.statementsGenerated()),
                    formatNumber(result.iterationsProcessed()),
                    formatNumber(result.iterationsDeduplicated()),
                    formatNumber(result.errorsEncountered()),
                    formatDuration(result.duration()),
                    result.completionReason());
        }
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Mapping {} checkpoint: {} statements (+{}), {} iterations (+{}) (total: {}, since last: {})",
                    mapping.getOriginalTriplesMap().getResourceName(),
                    formatNumber(checkpoint.totalStatementsGenerated()),
                    formatNumber(checkpoint.statementsGeneratedSinceLastCheckpoint()),
                    formatNumber(checkpoint.totalIterationsProcessed()),
                    formatNumber(checkpoint.iterationsProcessedSinceLastCheckpoint()),
                    formatDuration(checkpoint.totalDuration()),
                    formatDuration(checkpoint.timeSinceLastCheckpoint()));
        }
    }

    @Override
    public void onViewEvaluationStart(ResolvedMapping mapping, LogicalViewEvaluator evaluator) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "View {} evaluation started (evaluator: {})",
                    mapping.getEffectiveView().getResourceName(),
                    evaluator.getClass().getSimpleName());
        }
    }

    @Override
    public void onViewIteration(ResolvedMapping mapping, ViewIteration iteration) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "View {} iteration {} (keys: {})",
                    mapping.getEffectiveView().getResourceName(),
                    iteration.getIndex(),
                    iteration.getKeys());
        }
    }

    @Override
    public void onViewEvaluationComplete(ResolvedMapping mapping, long iterationCount, Duration duration) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "View {} evaluation completed: {} iterations ({})",
                    mapping.getEffectiveView().getResourceName(),
                    formatNumber(iterationCount),
                    formatDuration(duration));
        }
    }

    @Override
    public void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, TermMap termMap) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Mapping {} generated statement: {} (iteration: {}, termMap: {})",
                    mapping.getOriginalTriplesMap().getResourceName(),
                    statement,
                    source.getIndex(),
                    termMap.getResourceName());
        }
    }

    @Override
    public void onIterationDeduplicated(ResolvedMapping mapping, ViewIteration iteration) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Mapping {} deduplicated iteration {}",
                    mapping.getOriginalTriplesMap().getResourceName(),
                    iteration.getIndex());
        }
    }

    @Override
    public void onError(ResolvedMapping mapping, ViewIteration iteration, MappingError error) {
        if (LOG.isWarnEnabled()) {
            var mappingName = mapping.getOriginalTriplesMap().getResourceName();
            var iterationIndex = iteration != null ? String.valueOf(iteration.getIndex()) : "n/a";
            var expression = error.expression().orElse("n/a");

            if (error.cause().isPresent()) {
                LOG.warn(
                        "Mapping {} error at iteration {}: {} (expression: {})",
                        mappingName,
                        iterationIndex,
                        error.message(),
                        expression,
                        error.cause().get());
            } else {
                LOG.warn(
                        "Mapping {} error at iteration {}: {} (expression: {})",
                        mappingName,
                        iterationIndex,
                        error.message(),
                        expression);
            }
        }
    }

    /**
     * Formats a number with thousands separators for readability.
     *
     * @param number the number to format
     * @return the formatted number string (e.g. "1,250")
     */
    static String formatNumber(long number) {
        return String.format(Locale.US, "%,d", number);
    }

    /**
     * Formats a duration into a human-readable string. Uses the most significant unit(s) for
     * conciseness: milliseconds for sub-second durations, seconds with one decimal for durations
     * under a minute, and minutes with seconds for longer durations.
     *
     * @param duration the duration to format
     * @return a human-readable duration string (e.g. "1.2s", "2m 30s", "450ms")
     */
    static String formatDuration(Duration duration) {
        var totalMillis = duration.toMillis();
        if (totalMillis < 1000) {
            return "%dms".formatted(totalMillis);
        }

        var totalSeconds = duration.getSeconds();
        var millis = duration.toMillisPart();
        var tenths = (millis + 50) / 100;

        // Rounding up tenths may push us to the next second
        var effectiveSeconds = tenths >= 10 ? totalSeconds + 1 : totalSeconds;
        var effectiveTenths = tenths >= 10 ? 0 : tenths;

        if (effectiveSeconds < 60) {
            return "%d.%ds".formatted(effectiveSeconds, effectiveTenths);
        }

        var minutes = effectiveSeconds / 60;
        var seconds = effectiveSeconds % 60;
        if (seconds == 0) {
            return "%dm".formatted(minutes);
        }
        return "%dm %ds".formatted(minutes, seconds);
    }
}
