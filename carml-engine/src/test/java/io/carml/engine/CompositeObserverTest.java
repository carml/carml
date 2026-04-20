package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;
import org.junit.jupiter.api.Test;

class CompositeObserverTest {

    @Test
    void of_givenEmptyList_returnsNoOpObserver() {
        var result = CompositeObserver.of(List.of());

        assertThat(result, is(instanceOf(NoOpObserver.class)));
    }

    @Test
    void of_givenSingleObserver_returnsThatObserver() {
        var single = mock(MappingExecutionObserver.class);

        var result = CompositeObserver.of(List.of(single));

        assertThat(result, is(sameInstance(single)));
    }

    @Test
    void of_givenMultipleObservers_returnsCompositeObserver() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);

        var result = CompositeObserver.of(List.of(first, second));

        assertThat(result, is(instanceOf(CompositeObserver.class)));
    }

    @Test
    void onMappingStart_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onMappingStart(mapping);

        verify(first).onMappingStart(mapping);
        verify(second).onMappingStart(mapping);
    }

    @Test
    void onMappingComplete_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var result = new MappingExecutionResult(10L, 5L, 1L, 0L, Duration.ofSeconds(1), CompletionReason.EXHAUSTED);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onMappingComplete(mapping, result);

        verify(first).onMappingComplete(mapping, result);
        verify(second).onMappingComplete(mapping, result);
    }

    @Test
    void onCheckpoint_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var checkpoint = new CheckpointInfo(5L, 50L, 3L, 30L, Duration.ofSeconds(10), Duration.ofMinutes(1));

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onCheckpoint(mapping, checkpoint);

        verify(first).onCheckpoint(mapping, checkpoint);
        verify(second).onCheckpoint(mapping, checkpoint);
    }

    @Test
    void onViewEvaluationStart_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var evaluator = mock(LogicalViewEvaluator.class);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onViewEvaluationStart(mapping, evaluator);

        verify(first).onViewEvaluationStart(mapping, evaluator);
        verify(second).onViewEvaluationStart(mapping, evaluator);
    }

    @Test
    void onViewIteration_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var iteration = mock(ViewIteration.class);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onViewIteration(mapping, iteration);

        verify(first).onViewIteration(mapping, iteration);
        verify(second).onViewIteration(mapping, iteration);
    }

    @Test
    void onViewEvaluationComplete_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var duration = Duration.ofSeconds(42);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onViewEvaluationComplete(mapping, 100L, duration);

        verify(first).onViewEvaluationComplete(mapping, 100L, duration);
        verify(second).onViewEvaluationComplete(mapping, 100L, duration);
    }

    @Test
    void onStatementGenerated_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var iteration = mock(ViewIteration.class);
        var statement = mock(Statement.class);
        Set<LogicalTarget> logicalTargets = Set.of(mock(LogicalTarget.class));

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onStatementGenerated(mapping, iteration, statement, logicalTargets);

        verify(first).onStatementGenerated(mapping, iteration, statement, logicalTargets);
        verify(second).onStatementGenerated(mapping, iteration, statement, logicalTargets);
    }

    @Test
    void onIterationDeduplicated_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var iteration = mock(ViewIteration.class);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onIterationDeduplicated(mapping, iteration);

        verify(first).onIterationDeduplicated(mapping, iteration);
        verify(second).onIterationDeduplicated(mapping, iteration);
    }

    @Test
    void fanOut_givenThrowingDelegate_stillNotifiesRemainingDelegates() {
        var throwing = mock(MappingExecutionObserver.class);
        var healthy = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        doThrow(new RuntimeException("boom")).when(throwing).onMappingStart(mapping);

        var composite = CompositeObserver.of(List.of(throwing, healthy));
        composite.onMappingStart(mapping);

        verify(throwing).onMappingStart(mapping);
        verify(healthy).onMappingStart(mapping);
    }

    @Test
    void fanOut_givenMiddleThrowingDelegate_notifiesRemainingDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var throwing = mock(MappingExecutionObserver.class);
        var last = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        doThrow(new RuntimeException("boom")).when(throwing).onMappingStart(mapping);

        var composite = CompositeObserver.of(List.of(first, throwing, last));
        composite.onMappingStart(mapping);

        verify(first).onMappingStart(mapping);
        verify(throwing).onMappingStart(mapping);
        verify(last).onMappingStart(mapping);
    }

    @Test
    void of_givenNullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> CompositeObserver.of(null));
    }

    @Test
    void onError_fansOutToAllDelegates() {
        var first = mock(MappingExecutionObserver.class);
        var second = mock(MappingExecutionObserver.class);
        var mapping = mock(ResolvedMapping.class);
        var iteration = mock(ViewIteration.class);
        var error = MappingError.of("test error", null);

        var composite = CompositeObserver.of(List.of(first, second));
        composite.onError(mapping, iteration, error);

        verify(first).onError(mapping, iteration, error);
        verify(second).onError(mapping, iteration, error);
    }
}
