package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.model.LogicalTarget;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Statements;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Shared base for {@link MappedValue}/{@link MappingResult} implementations that describe an RDF
 * collection (list) or container (Bag/Seq/Alt). Holds the gathered element values plus any nested
 * {@link MappingResult}s contributed by gathered terms that are themselves mapped statements
 * (e.g. nested {@link RdfList}/{@link RdfContainer}). Statements are emitted lazily by
 * {@link #getResults()}, avoiding the LinkedHashModel materialization overhead that previously
 * caused OOM at million-element scale.
 *
 * <p>The {@code elements} and {@code nestedResults} fields are stored as references to mutable
 * {@link ArrayList}s so that mergeable subclasses can implement {@code merge} in
 * O(piece-size) by appending in place rather than reconstructing a new RDF model on every merge.
 *
 * <p><b>Mutation contract for {@link #getElements()} and {@link #getNestedResults()}:</b> the
 * returned lists are the live, mutable accumulator references. Only the {@code merge}
 * implementations of {@link MergeableRdfList} and {@link MergeableRdfContainer} are permitted to
 * mutate them (via {@code addAll}). All other callers — including engine code consuming the
 * mapping result, observers, and tests — must treat the returned lists as read-only. Mutating
 * them outside merge will silently corrupt subsequent reductions in
 * {@code RmlMapper#mergeMergeables} since merged instances share the same list reference.
 *
 * <p>The {@code graphs} field captures any graph context applied via {@code withGraphs(...)} on
 * the non-mergeable path. When non-empty, every emitted statement (own structural triples and
 * forwarded nested triples) is replicated once per graph context.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
@Getter
public abstract class AbstractRdfMappedStatementCollection<T extends Value>
        implements MappedValue<T>, MappingResult<Statement> {

    @Builder.Default
    private final List<Value> elements = new ArrayList<>();

    @Builder.Default
    private final List<MappingResult<Statement>> nestedResults = new ArrayList<>();

    @Singular
    private final Set<LogicalTarget> logicalTargets;

    @Builder.Default
    private final Set<Resource> graphs = Set.of();

    @Override
    public Set<LogicalTarget> getLogicalTargets() {
        return logicalTargets;
    }

    @Override
    public Publisher<Statement> getResults() {
        var nestedFlux = Flux.fromIterable(nestedResults).concatMap(nested -> Flux.from(nested.getResults()));

        var ownFlux = Flux.defer(() -> Flux.fromStream(buildOwnStatements()));

        var combined = nestedFlux.concatWith(ownFlux);

        if (graphs.isEmpty()) {
            return combined;
        }

        return combined.flatMap(stmt -> Flux.fromIterable(graphs)
                .map(graph -> Statements.statement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), graph)));
    }

    /**
     * Builds the structural statements that describe this collection itself (excluding nested-
     * collection statements, which are forwarded via {@link #getResults()}).
     *
     * @return a lazy stream of structural statements
     */
    protected abstract Stream<Statement> buildOwnStatements();

    /**
     * Computes the {@code (logicalTargets, graphs)} pair resulting from merging the given mapped
     * graphs into this collection. Subclasses use this to implement their {@code withGraphs(...)}
     * so the common logic is shared while the subclass-specific builder return type is preserved.
     *
     * @param mappedGraphs the mapped graph resources to merge in
     * @return merged logical targets and graph resources
     */
    protected GraphMerge mergeGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var newTargets = new LinkedHashSet<>(logicalTargets);
        mappedGraphs.stream().map(MappedValue::getLogicalTargets).forEach(newTargets::addAll);

        var newGraphs = new LinkedHashSet<>(graphs);
        mappedGraphs.stream().map(MappedValue::getValue).forEach(newGraphs::add);

        return new GraphMerge(Set.copyOf(newTargets), Set.copyOf(newGraphs));
    }

    protected record GraphMerge(Set<LogicalTarget> logicalTargets, Set<Resource> graphs) {}
}
