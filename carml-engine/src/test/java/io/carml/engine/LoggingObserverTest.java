package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LoggingObserverTest {

    private ResolvedMapping mapping;

    @BeforeEach
    void setUp() {
        mapping = mock(ResolvedMapping.class);
        var triplesMap = mock(TriplesMap.class);
        var view = mock(LogicalView.class);

        when(mapping.getOriginalTriplesMap()).thenReturn(triplesMap);
        when(mapping.getEffectiveView()).thenReturn(view);
        when(triplesMap.getResourceName()).thenReturn(":studentMapping");
        when(view.getResourceName()).thenReturn(":studentView");
    }

    @Test
    void create_returnsNonNullInstance() {
        var observer = LoggingObserver.create();

        assertThat(observer, is(notNullValue()));
        assertThat(observer, is(instanceOf(LoggingObserver.class)));
        assertThat(observer, is(instanceOf(MappingExecutionObserver.class)));
    }

    @Nested
    class CallbackSmokeTests {

        private LoggingObserver observer;

        @BeforeEach
        void setUp() {
            observer = LoggingObserver.create();
        }

        @Test
        void onMappingStart_doesNotThrow() {
            assertDoesNotThrow(() -> observer.onMappingStart(mapping));
        }

        @Test
        void onMappingComplete_doesNotThrow() {
            var result = new MappingExecutionResult(
                    1250L, 500L, 12L, 0L, Duration.ofMillis(1200), CompletionReason.EXHAUSTED);

            assertDoesNotThrow(() -> observer.onMappingComplete(mapping, result));
        }

        @Test
        void onCheckpoint_doesNotThrow() {
            var checkpoint = new CheckpointInfo(100L, 500L, 50L, 250L, Duration.ofSeconds(10), Duration.ofMinutes(1));

            assertDoesNotThrow(() -> observer.onCheckpoint(mapping, checkpoint));
        }

        @Test
        void onViewEvaluationStart_doesNotThrow() {
            var evaluator = mock(LogicalViewEvaluator.class);

            assertDoesNotThrow(() -> observer.onViewEvaluationStart(mapping, evaluator));
        }

        @Test
        void onViewIteration_doesNotThrow() {
            var iteration = mock(ViewIteration.class);
            when(iteration.getIndex()).thenReturn(0);
            when(iteration.getKeys()).thenReturn(Set.of("name", "age"));

            assertDoesNotThrow(() -> observer.onViewIteration(mapping, iteration));
        }

        @Test
        void onViewEvaluationComplete_doesNotThrow() {
            assertDoesNotThrow(() -> observer.onViewEvaluationComplete(mapping, 512L, Duration.ofMillis(800)));
        }

        @Test
        void onStatementGenerated_doesNotThrow() {
            var iteration = mock(ViewIteration.class);
            when(iteration.getIndex()).thenReturn(0);
            var statement = mock(Statement.class);
            Set<LogicalTarget> logicalTargets = Set.of(mock(LogicalTarget.class));

            assertDoesNotThrow(() -> observer.onStatementGenerated(mapping, iteration, statement, logicalTargets));
        }

        @Test
        void onStatementGenerated_withNullMappingAndNullSource_doesNotThrow() {
            // Given — post-merge firing path passes null for both ResolvedMapping and ViewIteration
            // (merged results aggregate across iterations; no single mapping/iteration is
            // meaningful). The observer must tolerate that without NPE.
            var statement = mock(Statement.class);
            Set<LogicalTarget> logicalTargets = Set.of(mock(LogicalTarget.class));

            assertDoesNotThrow(() -> observer.onStatementGenerated(null, null, statement, logicalTargets));
        }

        @Test
        void onIterationDeduplicated_doesNotThrow() {
            var iteration = mock(ViewIteration.class);
            when(iteration.getIndex()).thenReturn(5);

            assertDoesNotThrow(() -> observer.onIterationDeduplicated(mapping, iteration));
        }

        @Test
        void onError_withCause_doesNotThrow() {
            var iteration = mock(ViewIteration.class);
            when(iteration.getIndex()).thenReturn(3);
            var error = MappingError.of("field not found", "$.name", new RuntimeException("parse error"));

            assertDoesNotThrow(() -> observer.onError(mapping, iteration, error));
        }

        @Test
        void onError_withoutCause_doesNotThrow() {
            var iteration = mock(ViewIteration.class);
            when(iteration.getIndex()).thenReturn(3);
            var error = new MappingError("field not found", Optional.empty(), Optional.empty());

            assertDoesNotThrow(() -> observer.onError(mapping, iteration, error));
        }

        @Test
        void onError_withNullIteration_doesNotThrow() {
            var error = MappingError.of("initialization failed", null);

            assertDoesNotThrow(() -> observer.onError(mapping, null, error));
        }
    }

    @Nested
    class FormatNumberTests {

        @Test
        void formatNumber_zero() {
            assertThat(LoggingObserver.formatNumber(0), is("0"));
        }

        @Test
        void formatNumber_small() {
            assertThat(LoggingObserver.formatNumber(42), is("42"));
        }

        @Test
        void formatNumber_thousands() {
            assertThat(LoggingObserver.formatNumber(1250), is("1,250"));
        }

        @Test
        void formatNumber_millions() {
            assertThat(LoggingObserver.formatNumber(1_000_000), is("1,000,000"));
        }

        @Test
        void formatNumber_negative() {
            assertThat(LoggingObserver.formatNumber(-500), is("-500"));
        }
    }

    @Nested
    class FormatDurationTests {

        @ParameterizedTest
        @CsvSource({
            "0,     0ms",
            "1,     1ms",
            "500,   500ms",
            "999,   999ms",
            "1000,  1.0s",
            "1200,  1.2s",
            "1950,  2.0s",
            "5500,  5.5s",
            "10000, 10.0s",
            "59400, 59.4s",
            "1249,  1.2s",
            "1250,  1.3s",
        })
        void formatDuration_subMinute(long millis, String expected) {
            assertThat(LoggingObserver.formatDuration(Duration.ofMillis(millis)), is(expected));
        }

        @Test
        void formatDuration_roundsUpToMinute() {
            assertThat(LoggingObserver.formatDuration(Duration.ofMillis(59999)), is("1m"));
        }

        @Test
        void formatDuration_exactMinute() {
            assertThat(LoggingObserver.formatDuration(Duration.ofMinutes(1)), is("1m"));
        }

        @Test
        void formatDuration_minutesAndSeconds() {
            assertThat(LoggingObserver.formatDuration(Duration.ofSeconds(150)), is("2m 30s"));
        }

        @Test
        void formatDuration_multipleMinutes() {
            assertThat(LoggingObserver.formatDuration(Duration.ofMinutes(5)), is("5m"));
        }

        @Test
        void formatDuration_largerDuration() {
            assertThat(LoggingObserver.formatDuration(Duration.ofSeconds(3661)), is("61m 1s"));
        }
    }
}
