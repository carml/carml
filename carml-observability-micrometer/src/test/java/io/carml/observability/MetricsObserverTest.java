package io.carml.observability;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.engine.CheckpointInfo;
import io.carml.engine.CompletionReason;
import io.carml.engine.MappingError;
import io.carml.engine.MappingExecutionResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsObserverTest {

    private SimpleMeterRegistry registry;
    private MetricsObserver observer;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        observer = MetricsObserver.create(registry);
    }

    @Nested
    class ActiveMappingsGauge {

        @Test
        void onMappingStart_incrementsActiveGauge() {
            observer.onMappingStart(mapping("tm1"));

            assertThat(registry.get("carml.mappings.active").gauge().value(), is(1.0));
        }

        @Test
        void onMappingComplete_decrementsActiveGauge() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(10, 5, 0, Duration.ofMillis(100)));

            assertThat(registry.get("carml.mappings.active").gauge().value(), is(0.0));
        }

        @Test
        void multipleMappingsStart_tracksAllActive() {
            observer.onMappingStart(mapping("tm1"));
            observer.onMappingStart(mapping("tm2"));
            observer.onMappingStart(mapping("tm3"));

            assertThat(registry.get("carml.mappings.active").gauge().value(), is(3.0));
        }

        @Test
        void gaugeIsInstanceSpecific_secondObserverHasOwnGauge() {
            observer.onMappingStart(mapping("tm1"));

            var observer2 = MetricsObserver.create(registry);
            // Second observer's gauge reads 0 — the registry returns the first gauge,
            // but the second observer's activeMappings is independent
            assertThat(registry.get("carml.mappings.active").gauge().value(), is(1.0));

            observer2.onMappingStart(mapping("tm2"));
            // Gauge still reports from the observer instance that registered it
            assertThat(registry.get("carml.mappings.active").gauge().value(), is(1.0));
        }
    }

    @Nested
    class MappingDuration {

        @Test
        void onMappingComplete_recordsDuration() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(100, 50, 2, Duration.ofMillis(1500)));

            var timer = registry.get("carml.mapping.duration")
                    .tag("triples_map", "tm1")
                    .timer();
            assertThat(timer.count(), is(1L));
            assertThat(timer.totalTime(TimeUnit.MILLISECONDS), closeTo(1500.0, 1.0));
        }

        @Test
        void onMappingComplete_tagsByTriplesMap() {
            var rm1 = mapping("tm1");
            var rm2 = mapping("tm2");
            observer.onMappingStart(rm1);
            observer.onMappingComplete(rm1, result(10, 5, 0, Duration.ofMillis(100)));
            observer.onMappingStart(rm2);
            observer.onMappingComplete(rm2, result(20, 10, 0, Duration.ofMillis(200)));

            var timer1 = registry.get("carml.mapping.duration")
                    .tag("triples_map", "tm1")
                    .timer();
            var timer2 = registry.get("carml.mapping.duration")
                    .tag("triples_map", "tm2")
                    .timer();

            assertThat(timer1.count(), is(1L));
            assertThat(timer2.count(), is(1L));
            assertThat(timer2.totalTime(TimeUnit.MILLISECONDS), closeTo(200.0, 1.0));
        }
    }

    @Nested
    class StatementCounter {

        @Test
        void onMappingComplete_recordsStatementCount() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(1250, 500, 12, Duration.ofSeconds(1)));

            var counter = registry.get("carml.statements.generated")
                    .tag("triples_map", "tm1")
                    .counter();
            assertThat(counter.count(), is(1250.0));
        }

        @Test
        void onMappingComplete_incrementsGlobalTotal() {
            var rm1 = mapping("tm1");
            var rm2 = mapping("tm2");
            observer.onMappingStart(rm1);
            observer.onMappingComplete(rm1, result(100, 50, 0, Duration.ofMillis(50)));
            observer.onMappingStart(rm2);
            observer.onMappingComplete(rm2, result(200, 100, 0, Duration.ofMillis(50)));

            var total = registry.get("carml.statements.total").counter();
            assertThat(total.count(), is(300.0));
        }
    }

    @Nested
    class IterationCounters {

        @Test
        void onViewIteration_incrementsProcessedCounter() {
            var rm = mapping("tm1");
            var evaluator = mock(LogicalViewEvaluator.class);
            var iteration = mock(ViewIteration.class);

            observer.onViewEvaluationStart(rm, evaluator);
            observer.onViewIteration(rm, iteration);
            observer.onViewIteration(rm, iteration);
            observer.onViewIteration(rm, iteration);

            var counter = registry.get("carml.iterations.processed")
                    .tag("triples_map", "tm1")
                    .counter();
            assertThat(counter.count(), is(3.0));
        }

        @Test
        void onViewIteration_withoutPriorEvaluationStart_usesUnknownEvaluator() {
            var rm = mapping("tm1");
            var iteration = mock(ViewIteration.class);

            observer.onViewIteration(rm, iteration);

            var counter = registry.get("carml.iterations.processed")
                    .tag("triples_map", "tm1")
                    .tag("evaluator", "unknown")
                    .counter();
            assertThat(counter.count(), is(1.0));
        }

        @Test
        void onMappingComplete_recordsDeduplicatedCount() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(100, 50, 8, Duration.ofSeconds(1)));

            var counter = registry.get("carml.iterations.deduplicated")
                    .tag("triples_map", "tm1")
                    .counter();
            assertThat(counter.count(), is(8.0));
        }
    }

    @Nested
    class ViewEvaluationDuration {

        @Test
        void onViewEvaluationComplete_recordsDurationWithTags() {
            var rm = mapping("tm1");
            var evaluator = mock(LogicalViewEvaluator.class);

            observer.onViewEvaluationStart(rm, evaluator);
            observer.onViewEvaluationComplete(rm, 500, Duration.ofMillis(800));

            var timer = registry.get("carml.view.evaluation.duration")
                    .tag("view", "testView")
                    .timer();
            assertThat(timer, notNullValue());
            assertThat(timer.count(), is(1L));
            assertThat(timer.totalTime(TimeUnit.MILLISECONDS), closeTo(800.0, 1.0));
        }
    }

    @Nested
    class DistributionSummaries {

        @Test
        void onMappingComplete_recordsStatementsPerExecution() {
            var rm1 = mapping("tm1");
            observer.onMappingStart(rm1);
            observer.onMappingComplete(rm1, result(100, 50, 0, Duration.ofMillis(50)));

            var rm2 = mapping("tm1");
            observer.onMappingStart(rm2);
            observer.onMappingComplete(rm2, result(200, 100, 0, Duration.ofMillis(50)));

            var summary = registry.get("carml.mapping.statements")
                    .tag("triples_map", "tm1")
                    .summary();
            assertThat(summary.count(), is(2L));
            assertThat(summary.totalAmount(), is(300.0));
            assertThat(summary.mean(), is(150.0));
        }

        @Test
        void onViewEvaluationComplete_recordsIterationsPerEvaluation() {
            var rm = mapping("tm1");
            var evaluator = mock(LogicalViewEvaluator.class);

            observer.onViewEvaluationStart(rm, evaluator);
            observer.onViewEvaluationComplete(rm, 500, Duration.ofMillis(100));

            observer.onViewEvaluationStart(rm, evaluator);
            observer.onViewEvaluationComplete(rm, 1500, Duration.ofMillis(200));

            var summary = registry.get("carml.view.evaluation.iterations")
                    .tag("view", "testView")
                    .summary();
            assertThat(summary.count(), is(2L));
            assertThat(summary.totalAmount(), is(2000.0));
            assertThat(summary.mean(), is(1000.0));
        }
    }

    @Nested
    class ErrorCounter {

        @Test
        void onError_incrementsErrorCounter() {
            var rm = mapping("tm1");
            var error = mock(MappingError.class);

            observer.onError(rm, null, error);
            observer.onError(rm, null, error);

            var counter = registry.get("carml.errors").tag("triples_map", "tm1").counter();
            assertThat(counter.count(), is(2.0));
        }
    }

    @Nested
    class CompletionCounter {

        @Test
        void onMappingComplete_recordsCompletionWithReason() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(10, 5, 0, Duration.ofMillis(50)));

            var counter = registry.get("carml.mapping.completed")
                    .tag("triples_map", "tm1")
                    .tag("reason", "EXHAUSTED")
                    .counter();
            assertThat(counter.count(), is(1.0));
        }

        @Test
        void onMappingComplete_distinguishesReasons() {
            var rm1 = mapping("tm1");
            observer.onMappingStart(rm1);
            observer.onMappingComplete(rm1, result(10, 5, 0, Duration.ofMillis(50)));

            var rm2 = mapping("tm2");
            observer.onMappingStart(rm2);
            observer.onMappingComplete(
                    rm2, new MappingExecutionResult(0, 0, 0, 1, Duration.ofMillis(10), CompletionReason.ERROR));

            var exhausted = registry.get("carml.mapping.completed")
                    .tag("reason", "EXHAUSTED")
                    .counter();
            var error = registry.get("carml.mapping.completed")
                    .tag("reason", "ERROR")
                    .counter();
            assertThat(exhausted.count(), is(1.0));
            assertThat(error.count(), is(1.0));
        }
    }

    @Nested
    class Checkpoint {

        @Test
        void checkpointThenComplete_countsOnlyFromResult() {
            var rm = mapping("tm1");
            observer.onMappingStart(rm);

            // Streaming: checkpoints fire during execution
            observer.onCheckpoint(rm, checkpoint(500, 500));
            observer.onCheckpoint(rm, checkpoint(300, 800));

            // Completion: result has the cumulative total
            observer.onMappingComplete(rm, result(800, 400, 0, Duration.ofSeconds(2)));

            // Per-TriplesMap counter: only from onMappingComplete, no checkpoint double-counting
            var perMapping = registry.get("carml.statements.generated")
                    .tag("triples_map", "tm1")
                    .counter();
            assertThat(perMapping.count(), is(800.0));

            // Global total: also only from onMappingComplete
            var total = registry.get("carml.statements.total").counter();
            assertThat(total.count(), is(800.0));
        }
    }

    @Nested
    class NullResourceNames {

        @Test
        void nullTriplesMapName_fallsBackToUnknown() {
            var rm = mapping(null);
            observer.onMappingStart(rm);
            observer.onMappingComplete(rm, result(10, 5, 0, Duration.ofMillis(50)));

            var timer = registry.get("carml.mapping.duration")
                    .tag("triples_map", "unknown")
                    .timer();
            assertThat(timer.count(), is(1L));
        }
    }

    // --- Helpers ---

    private static ResolvedMapping mapping(String triplesMapName) {
        var triplesMap = mock(TriplesMap.class);
        lenient().when(triplesMap.getResourceName()).thenReturn(triplesMapName);

        var view = mock(LogicalView.class);
        lenient().when(view.getResourceName()).thenReturn("testView");

        var mapping = mock(ResolvedMapping.class);
        lenient().when(mapping.getOriginalTriplesMap()).thenReturn(triplesMap);
        lenient().when(mapping.getEffectiveView()).thenReturn(view);
        return mapping;
    }

    private static MappingExecutionResult result(
            long statements, long iterations, long deduplicated, Duration duration) {
        return new MappingExecutionResult(
                statements, iterations, deduplicated, 0, duration, CompletionReason.EXHAUSTED);
    }

    private static CheckpointInfo checkpoint(long statementsDelta, long statementsTotal) {
        return new CheckpointInfo(statementsDelta, statementsTotal, 0, 0, Duration.ZERO, Duration.ZERO);
    }
}
