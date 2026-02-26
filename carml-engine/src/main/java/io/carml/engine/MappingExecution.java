package io.carml.engine;

import java.util.List;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Lifecycle handle for a mapping execution. Provides access to the produced RDF statement stream and
 * control operations for long-running or streaming executions.
 *
 * <p>Implementations wrap the reactive mapping pipeline and expose methods for cancellation,
 * checkpointing (for streaming fault tolerance), and runtime metrics inspection.
 */
public interface MappingExecution {

    /**
     * Creates a new {@link MappingExecution} wrapping the given statement flux with lifecycle
     * management. The returned handle tracks statement and error counts, supports cancellation, and
     * fires observer checkpoint callbacks.
     *
     * @param statements the reactive stream of RDF statements to wrap
     * @param observer the observer to receive checkpoint callbacks
     * @param resolvedMappings the resolved mappings for checkpoint context
     * @return a new {@link MappingExecution} lifecycle handle
     */
    static MappingExecution of(
            Flux<Statement> statements, MappingExecutionObserver observer, List<ResolvedMapping> resolvedMappings) {
        return new DefaultMappingExecution(statements, observer, resolvedMappings);
    }

    /**
     * The reactive stream of RDF statements produced by this execution.
     *
     * @return a {@link Flux} of {@link Statement} instances
     */
    Flux<Statement> statements();

    /**
     * Cancel the execution. The returned {@link Mono} completes when cancellation has been
     * processed.
     *
     * @return a {@link Mono} that completes when cancellation is done
     */
    Mono<Void> cancel();

    /**
     * Trigger a checkpoint for fault tolerance. The returned {@link Mono} completes when the
     * checkpoint has been persisted.
     *
     * @return a {@link Mono} that completes when the checkpoint is persisted
     */
    Mono<Void> checkpoint();

    /**
     * Get current execution metrics.
     *
     * @return the current {@link MappingExecutionMetrics} snapshot
     */
    MappingExecutionMetrics currentMetrics();
}
