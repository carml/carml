package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import org.junit.jupiter.api.Test;

class NoOpObserverTest {

    @Test
    void getInstance_returnsSameInstance() {
        var first = NoOpObserver.getInstance();
        var second = NoOpObserver.getInstance();

        assertThat(first, is(sameInstance(second)));
    }

    @Test
    void allCallbacks_withNullArgs_doNotThrow() {
        var observer = NoOpObserver.getInstance();

        // All default methods are no-ops; calling them with null should not throw
        observer.onMappingStart(null);
        observer.onMappingComplete(null, null);
        observer.onCheckpoint(null, null);
        observer.onViewEvaluationStart(null, null);
        observer.onViewIteration(null, null);
        observer.onViewEvaluationComplete(null, 0, null);
        observer.onStatementGenerated(null, null, null, java.util.Set.of());
        observer.onIterationDeduplicated(null, null);
        observer.onError(null, null, null);
    }
}
