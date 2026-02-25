package io.carml.engine;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.TermMap;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Statement;

/**
 * Observer that fans out each callback to multiple delegate observers. Used internally when multiple
 * observers are registered via the mapper builder.
 *
 * <p>The {@link #of(List)} factory method optimizes for common cases: an empty list returns
 * {@link NoOpObserver}, a single-element list returns the observer directly (unwrapped), and
 * multiple observers are wrapped in a {@code CompositeObserver}.
 *
 * <p>Exception isolation: if a delegate throws during a callback, the exception is logged and
 * remaining delegates still receive the callback. This prevents a misbehaving observer from
 * suppressing notifications to other observers.
 */
@Slf4j
public final class CompositeObserver implements MappingExecutionObserver {

    private final List<MappingExecutionObserver> delegates;

    private CompositeObserver(List<MappingExecutionObserver> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    /**
     * Creates an observer from a list of delegates, optimizing for common cases.
     *
     * @param observers the delegate observers
     * @return a {@link NoOpObserver} if the list is empty, the single observer if the list has one
     *     element, or a {@code CompositeObserver} wrapping all delegates
     */
    public static MappingExecutionObserver of(List<MappingExecutionObserver> observers) {
        if (observers.isEmpty()) {
            return NoOpObserver.getInstance();
        }
        if (observers.size() == 1) {
            return observers.get(0);
        }
        return new CompositeObserver(observers);
    }

    private void fanOut(Consumer<MappingExecutionObserver> action) {
        for (var delegate : delegates) {
            try {
                action.accept(delegate);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Observer {} threw on callback; continuing with remaining observers",
                        delegate.getClass().getName(),
                        ex);
            }
        }
    }

    // --- Mapping lifecycle ---

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        fanOut(d -> d.onMappingStart(mapping));
    }

    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        fanOut(d -> d.onMappingComplete(mapping, result));
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        fanOut(d -> d.onCheckpoint(mapping, checkpoint));
    }

    // --- View evaluation ---

    @Override
    public void onViewEvaluationStart(ResolvedMapping mapping, LogicalViewEvaluator evaluator) {
        fanOut(d -> d.onViewEvaluationStart(mapping, evaluator));
    }

    @Override
    public void onViewIteration(ResolvedMapping mapping, ViewIteration iteration) {
        fanOut(d -> d.onViewIteration(mapping, iteration));
    }

    @Override
    public void onViewEvaluationComplete(ResolvedMapping mapping, long iterationCount, Duration duration) {
        fanOut(d -> d.onViewEvaluationComplete(mapping, iterationCount, duration));
    }

    // --- RDF generation ---

    @Override
    public void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, TermMap termMap) {
        fanOut(d -> d.onStatementGenerated(mapping, source, statement, termMap));
    }

    @Override
    public void onIterationDeduplicated(ResolvedMapping mapping, ViewIteration iteration) {
        fanOut(d -> d.onIterationDeduplicated(mapping, iteration));
    }

    // --- Errors ---

    @Override
    public void onError(ResolvedMapping mapping, ViewIteration iteration, MappingError error) {
        fanOut(d -> d.onError(mapping, iteration, error));
    }
}
