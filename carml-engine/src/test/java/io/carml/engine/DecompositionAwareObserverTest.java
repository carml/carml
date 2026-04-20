package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DecompositionAwareObserverTest {

    @Mock
    private MappingExecutionObserver delegate;

    @Test
    void wrap_givenNoDecomposition_returnsOriginalObserver() {
        var triplesMap = mock(TriplesMap.class);
        var mapping = buildResolvedMapping(triplesMap, true);

        var result = DecompositionAwareObserver.wrap(delegate, List.of(mapping));

        assertThat(result, is(sameInstance(delegate)));
    }

    @Test
    void onMappingStart_givenTwoSubGroups_forwardsOnlyOnce() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        observer.onMappingStart(mapping0);
        observer.onMappingStart(mapping1);

        // Delegate should have received exactly one onMappingStart call
        verify(delegate).onMappingStart(any());
    }

    @Test
    void onMappingComplete_givenTwoSubGroups_forwardsOnceWithAggregatedResult() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        var result0 = new MappingExecutionResult(10, 5, 1, 0, Duration.ofMillis(100), CompletionReason.EXHAUSTED);
        var result1 = new MappingExecutionResult(20, 8, 2, 1, Duration.ofMillis(200), CompletionReason.EXHAUSTED);

        observer.onMappingComplete(mapping0, result0);
        observer.onMappingComplete(mapping1, result1);

        var resultCaptor = ArgumentCaptor.forClass(MappingExecutionResult.class);
        verify(delegate).onMappingComplete(any(), resultCaptor.capture());

        var aggregated = resultCaptor.getValue();
        assertThat(aggregated.statementsGenerated(), is(30L));
        assertThat(aggregated.iterationsProcessed(), is(13L));
        assertThat(aggregated.iterationsDeduplicated(), is(3L));
        assertThat(aggregated.errorsEncountered(), is(1L));
        assertThat(aggregated.duration(), is(Duration.ofMillis(200)));
    }

    @Test
    void onViewEvaluationStart_givenTwoSubGroups_forwardsOnlyOnce() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        var evaluator = mock(LogicalViewEvaluator.class);

        observer.onViewEvaluationStart(mapping0, evaluator);
        observer.onViewEvaluationStart(mapping1, evaluator);

        verify(delegate).onViewEvaluationStart(any(), eq(evaluator));
    }

    @Test
    void onViewEvaluationComplete_givenTwoSubGroups_forwardsOnceWithAggregatedCounts() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        observer.onViewEvaluationComplete(mapping0, 100, Duration.ofMillis(50));
        observer.onViewEvaluationComplete(mapping1, 200, Duration.ofMillis(80));

        var countCaptor = ArgumentCaptor.forClass(Long.class);
        var durationCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(delegate).onViewEvaluationComplete(any(), countCaptor.capture(), durationCaptor.capture());

        assertThat(countCaptor.getValue(), is(300L));
        assertThat(durationCaptor.getValue(), is(Duration.ofMillis(80)));
    }

    @Test
    void onViewIteration_alwaysForwarded() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        var iteration1 = mock(ViewIteration.class);
        var iteration2 = mock(ViewIteration.class);

        observer.onViewIteration(mapping0, iteration1);
        observer.onViewIteration(mapping1, iteration2);

        // Both calls should be forwarded (no aggregation for onViewIteration)
        verify(delegate).onViewIteration(any(), eq(iteration1));
        verify(delegate).onViewIteration(any(), eq(iteration2));
    }

    @Test
    void onStatementGenerated_alwaysForwarded() {
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        var iteration = mock(ViewIteration.class);
        var statement1 = mock(Statement.class);
        var statement2 = mock(Statement.class);
        Set<LogicalTarget> logicalTargets = Set.of(mock(LogicalTarget.class));

        observer.onStatementGenerated(mapping0, iteration, statement1, logicalTargets);
        observer.onStatementGenerated(mapping1, iteration, statement2, logicalTargets);

        // Both calls should be forwarded (no aggregation for onStatementGenerated)
        verify(delegate).onStatementGenerated(any(), eq(iteration), eq(statement1), eq(logicalTargets));
        verify(delegate).onStatementGenerated(any(), eq(iteration), eq(statement2), eq(logicalTargets));
    }

    @Test
    void onStatementGenerated_withNullMapping_forwardsThroughWithoutAggregation() {
        // Given — post-merge firing path (RdfRmlMapper#wrapMergedForObserver) passes null for
        // ResolvedMapping because merged results aggregate across iterations and often across
        // decomposed sub-mappings; no single mapping is meaningful. The wrapper must not NPE on
        // isAggregated(mapping) and must forward the call through to the delegate unchanged.
        var triplesMap = mock(TriplesMap.class);
        var mapping0 = buildResolvedMapping(triplesMap, true);
        var mapping1 = buildResolvedMapping(triplesMap, false);

        var observer = DecompositionAwareObserver.wrap(delegate, List.of(mapping0, mapping1));

        var statement = mock(Statement.class);
        Set<LogicalTarget> logicalTargets = Set.of(mock(LogicalTarget.class));

        observer.onStatementGenerated(null, null, statement, logicalTargets);

        verify(delegate).onStatementGenerated(isNull(), isNull(), eq(statement), eq(logicalTargets));
    }

    /**
     * Builds a ResolvedMapping with the given TriplesMap and emitsClassTriples flag. Uses
     * DefaultResolvedMapping's builder since that is the concrete implementation.
     */
    private static ResolvedMapping buildResolvedMapping(TriplesMap triplesMap, boolean emitsClassTriples) {
        var view = mock(LogicalView.class);
        var evaluationContext = EvaluationContext.defaults();

        return DefaultResolvedMapping.builder()
                .originalTriplesMap(triplesMap)
                .effectiveView(view)
                .implicitView(false)
                .evaluationContext(evaluationContext)
                .fieldOrigins(Map.of())
                .dependencies(Set.of())
                .emitsClassTriples(emitsClassTriples)
                .build();
    }
}
