package io.carml.engine.rdf;

import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Decorates a {@link MappingResult} of {@link Statement}s so that every emitted statement fires
 * {@link MappingExecutionObserver#onStatementGenerated} as a side effect of subscription. The
 * delegate's {@link #getLogicalTargets()} is read per invocation and forwarded to the observer
 * together with the {@link Statement} content unchanged.
 *
 * <p>Applied at two distinct firing points:
 *
 * <ul>
 *   <li><strong>Per-iteration</strong> — see
 *       {@link RdfTriplesMapper#instrumentWithObserver(MappingResult, ViewIteration)}. Regular
 *       (non-mergeable) {@link MappingResult} instances produced during view iteration are wrapped
 *       here so the observer fires with the originating {@link ResolvedMapping} and
 *       {@link ViewIteration} source. Mergeable results are intentionally left unwrapped at this
 *       point to preserve the {@code instanceof MergeableMappingResult} check in
 *       {@code RmlMapper.handleCompletable}, which queues them for cross-iteration
 *       merging.
 *   <li><strong>Post-merge</strong> — see
 *       {@link io.carml.engine.rdf.RdfRmlMapper#wrapMergedForObserver(MappingResult)}. After merge
 *       reduction in {@code RmlMapper.mergeMergeables()}, the merged output is wrapped here so
 *       that rdf:List / rdf:Container statements (from {@code rml:gather}) fire the observer and
 *       are therefore routed to any declared logical targets. Merged results aggregate across
 *       iterations (often across decomposed sub-mappings), so both {@link ResolvedMapping} and
 *       the {@link ViewIteration} source are {@code null} on this path — observers consuming
 *       either field must tolerate {@code null}. The {@link #getLogicalTargets()} set surfaced on
 *       this path is the union of targets carried by all merged-in pieces, which
 *       {@code MergeableRdfList.merge} / {@code MergeableRdfContainer.merge} propagate.
 * </ul>
 *
 * <p><strong>Subscription semantics:</strong> observer firing is tied to {@link Flux}
 * subscription. Each subscription to {@link #getResults()} fires the observer for every emitted
 * {@link Statement}, so downstream operators that resubscribe (e.g. {@code retry}, {@code repeat})
 * would cause the observer to fire multiple times per statement. The engine's current pipeline
 * uses single-subscription semantics; instrumenting a stage that resubscribes would require a
 * different design (e.g. a cache operator or a per-subscription guard).
 */
final class ObserverFiringMappingResult<T extends Statement> implements MappingResult<T> {

    private final MappingResult<T> delegate;

    private final ResolvedMapping resolvedMapping;

    private final ViewIteration source;

    private final MappingExecutionObserver observer;

    ObserverFiringMappingResult(
            MappingResult<T> delegate,
            ResolvedMapping resolvedMapping,
            ViewIteration source,
            MappingExecutionObserver observer) {
        this.delegate = delegate;
        this.resolvedMapping = resolvedMapping;
        this.source = source;
        this.observer = observer;
    }

    @Override
    public Set<LogicalTarget> getLogicalTargets() {
        return delegate.getLogicalTargets();
    }

    @Override
    public Publisher<T> getResults() {
        var logicalTargets = delegate.getLogicalTargets();
        return Flux.from(delegate.getResults())
                .doOnNext(
                        statement -> observer.onStatementGenerated(resolvedMapping, source, statement, logicalTargets));
    }
}
