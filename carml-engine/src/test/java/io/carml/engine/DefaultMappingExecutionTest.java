package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class DefaultMappingExecutionTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static Statement testStatement(String suffix) {
        return VF.createStatement(
                VF.createIRI("http://example.com/s" + suffix),
                VF.createIRI("http://example.com/p"),
                VF.createIRI("http://example.com/o" + suffix));
    }

    @Test
    void givenSourceFlux_whenStatements_thenEmitsAllElements() {
        // Given
        var s1 = testStatement("1");
        var s2 = testStatement("2");
        var execution = MappingExecution.of(Flux.just(s1, s2), NoOpObserver.getInstance(), List.of());

        // When / Then
        StepVerifier.create(execution.statements())
                .expectNext(s1, s2)
                .expectComplete()
                .verify();
    }

    @Test
    void givenExecution_whenCancel_thenFluxCompletes() {
        // Given
        var execution = MappingExecution.of(Flux.never(), NoOpObserver.getInstance(), List.of());

        // When / Then
        StepVerifier.create(execution.statements())
                .then(() -> execution.cancel().subscribe())
                .expectComplete()
                .verify();
    }

    @Test
    void givenAlreadyCancelled_whenCancelAgain_thenNoError() {
        // Given
        var execution = MappingExecution.of(Flux.never(), NoOpObserver.getInstance(), List.of());

        // When / Then — cancel twice, both complete normally
        StepVerifier.create(execution.cancel()).expectComplete().verify();
        StepVerifier.create(execution.cancel()).expectComplete().verify();
    }

    @Test
    void givenExecution_whenCheckpoint_thenObserverNotifiedPerMapping() {
        // Given
        var observer = mock(MappingExecutionObserver.class);
        var mapping1 = mock(ResolvedMapping.class);
        var mapping2 = mock(ResolvedMapping.class);
        var execution = MappingExecution.of(Flux.just(testStatement("1")), observer, List.of(mapping1, mapping2));

        execution.statements().blockLast();

        // When
        execution.checkpoint().block();

        // Then
        verify(observer).onCheckpoint(eq(mapping1), any(CheckpointInfo.class));
        verify(observer).onCheckpoint(eq(mapping2), any(CheckpointInfo.class));
    }

    @Test
    void givenTwoCheckpoints_whenSecondCheckpoint_thenIncrementalCountersCorrect() {
        // Given
        var observer = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var execution = MappingExecution.of(
                Flux.just(testStatement("1"), testStatement("2"), testStatement("3")), observer, List.of(mapping));

        execution.statements().blockLast();

        // First checkpoint: all 3 statements
        execution.checkpoint().block();

        // When — second checkpoint: no new statements since flux completed
        execution.checkpoint().block();

        // Then
        var captor = ArgumentCaptor.forClass(CheckpointInfo.class);
        verify(observer, times(2)).onCheckpoint(eq(mapping), captor.capture());
        var checkpoints = captor.getAllValues();

        // First checkpoint: 3 incremental, 3 cumulative
        assertThat(checkpoints.get(0).statementsGeneratedSinceLastCheckpoint(), is(3L));
        assertThat(checkpoints.get(0).totalStatementsGenerated(), is(3L));
        assertThat(checkpoints.get(0).iterationsProcessedSinceLastCheckpoint(), is(0L));
        assertThat(checkpoints.get(0).totalIterationsProcessed(), is(0L));

        // Second checkpoint: 0 incremental, 3 cumulative
        assertThat(checkpoints.get(1).statementsGeneratedSinceLastCheckpoint(), is(0L));
        assertThat(checkpoints.get(1).totalStatementsGenerated(), is(3L));
        assertThat(checkpoints.get(1).iterationsProcessedSinceLastCheckpoint(), is(0L));
        assertThat(checkpoints.get(1).totalIterationsProcessed(), is(0L));

        // Monotonicity: second totalDuration >= first totalDuration
        assertThat(
                checkpoints.get(1).totalDuration().compareTo(checkpoints.get(0).totalDuration()) >= 0, is(true));
    }

    @Test
    void givenExecution_whenCurrentMetricsBeforeSubscription_thenZeroCounts() {
        // Given
        var execution = MappingExecution.of(Flux.just(testStatement("1")), NoOpObserver.getInstance(), List.of());

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.statementsProduced(), is(0L));
        assertThat(metrics.errorsEncountered(), is(0L));
    }

    @Test
    void givenConsumedStatements_whenCurrentMetrics_thenCorrectStatementCount() {
        // Given
        var execution = MappingExecution.of(
                Flux.just(testStatement("1"), testStatement("2"), testStatement("3")),
                NoOpObserver.getInstance(),
                List.of());

        execution.statements().blockLast();

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.statementsProduced(), is(3L));
    }

    @Test
    void givenFluxWithError_whenCurrentMetrics_thenErrorCounted() {
        // Given
        var execution = MappingExecution.of(
                Flux.concat(Flux.just(testStatement("1")), Flux.error(new RuntimeException("test error"))),
                NoOpObserver.getInstance(),
                List.of());

        // Consume and handle the error
        execution.statements().onErrorResume(ex -> Flux.empty()).blockLast();

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.statementsProduced(), is(1L));
        assertThat(metrics.errorsEncountered(), is(1L));
    }

    @Test
    void givenExecution_whenCurrentMetrics_thenStartedAtIsNotNull() {
        // Given
        var execution = MappingExecution.of(Flux.empty(), NoOpObserver.getInstance(), List.of());

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.startedAt(), is(notNullValue()));
    }

    @Test
    void givenExecution_whenCurrentMetrics_thenElapsedIsNonNegative() {
        // Given
        var execution = MappingExecution.of(Flux.empty(), NoOpObserver.getInstance(), List.of());

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.elapsed().isNegative(), is(false));
    }

    @Test
    void givenNoResolvedMappings_whenCheckpoint_thenObserverNotCalled() {
        // Given
        var observer = mock(MappingExecutionObserver.class);
        var execution = MappingExecution.of(Flux.just(testStatement("1")), observer, List.of());

        execution.statements().blockLast();

        // When
        execution.checkpoint().block();

        // Then
        verify(observer, never()).onCheckpoint(any(), any());
    }

    @Test
    void givenCancelledBeforeSubscribe_whenStatements_thenFluxCompletesImmediately() {
        // Given
        var execution = MappingExecution.of(Flux.never(), NoOpObserver.getInstance(), List.of());
        execution.cancel().block();

        // When / Then
        StepVerifier.create(execution.statements()).expectComplete().verify();
    }

    @Test
    void givenFluxWithError_whenErrorPropagates_thenBothCountersCorrect() {
        // Given
        var execution = MappingExecution.of(
                Flux.concat(
                        Flux.just(testStatement("1"), testStatement("2")), Flux.error(new RuntimeException("boom"))),
                NoOpObserver.getInstance(),
                List.of());

        // When — let error propagate through StepVerifier
        StepVerifier.create(execution.statements())
                .expectNextCount(2)
                .expectError(RuntimeException.class)
                .verify();

        // Then
        assertThat(execution.currentMetrics().statementsProduced(), is(2L));
        assertThat(execution.currentMetrics().errorsEncountered(), is(1L));
    }

    @Test
    void givenCancelAfterSomeElements_whenCurrentMetrics_thenCountReflectsConsumedStatements() {
        // Given — use unicast sink for deterministic element delivery
        var sink = Sinks.many().unicast().<Statement>onBackpressureBuffer();
        var execution = MappingExecution.of(sink.asFlux(), NoOpObserver.getInstance(), List.of());

        // When — emit 2 elements, then cancel
        StepVerifier.create(execution.statements())
                .then(() -> {
                    sink.tryEmitNext(testStatement("1"));
                    sink.tryEmitNext(testStatement("2"));
                })
                .expectNext(testStatement("1"), testStatement("2"))
                .then(() -> execution.cancel().subscribe())
                .expectComplete()
                .verify();

        // Then
        assertThat(execution.currentMetrics().statementsProduced(), is(2L));
    }

    @Test
    void givenCheckpointBeforeSubscription_whenCheckpoint_thenZeroCountsReported() {
        // Given
        var observer = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var execution = MappingExecution.of(Flux.just(testStatement("1")), observer, List.of(mapping));

        // When — checkpoint before subscribing to statements
        execution.checkpoint().block();

        // Then
        var captor = ArgumentCaptor.forClass(CheckpointInfo.class);
        verify(observer).onCheckpoint(eq(mapping), captor.capture());
        assertThat(captor.getValue().statementsGeneratedSinceLastCheckpoint(), is(0L));
        assertThat(captor.getValue().totalStatementsGenerated(), is(0L));
    }

    @Test
    void givenNullSourceFlux_whenOf_thenThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class, () -> MappingExecution.of(null, NoOpObserver.getInstance(), List.of()));
    }

    @Test
    void givenNullObserver_whenOf_thenThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> MappingExecution.of(Flux.empty(), null, List.of()));
    }

    @Test
    void givenNullResolvedMappings_whenOf_thenThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class, () -> MappingExecution.of(Flux.empty(), NoOpObserver.getInstance(), null));
    }

    @Test
    void givenCheckpoint_whenCheckpointTimeDurations_thenDurationsNonNegative() {
        // Given
        var observer = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var execution = MappingExecution.of(Flux.just(testStatement("1")), observer, List.of(mapping));

        execution.statements().blockLast();

        // When
        execution.checkpoint().block();

        // Then
        var captor = ArgumentCaptor.forClass(CheckpointInfo.class);
        verify(observer).onCheckpoint(eq(mapping), captor.capture());
        var checkpoint = captor.getValue();

        assertThat(checkpoint.timeSinceLastCheckpoint().isNegative(), is(false));
        assertThat(checkpoint.totalDuration().isNegative(), is(false));
    }
}
