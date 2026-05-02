package io.carml.engine.rdf.cc;

import io.carml.engine.MergeableMappingResult;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Shared streaming and merge helpers for {@link MergeableRdfList} and {@link MergeableRdfContainer}.
 * Both classes share an identical statement-emission shape (forward nested results, then own
 * structural triples, optional per-graph blank-node remap, optional linking triples) and an
 * identical merge contract (in-place mutation of element/nested accumulators plus union of linking
 * sets). The differences live solely in their parent class (head vs container) and Lombok-generated
 * builders, both of which are accessed reflexively via {@link AbstractRdfMappedStatementCollection}
 * accessors and per-class {@code toBuilder()} calls in the subclass.
 */
@UtilityClass
class MergeableCollectionStreams {

    /**
     * Returns the merge key for a graph-aware mergeable collection: the value itself for
     * default-graph mergeables, a {@link GraphScopedMergeKey} otherwise. Mergeables with the same
     * value but different graph contexts must produce different keys so {@code mergeMergeables}
     * doesn't fold them into a single accumulator.
     */
    static Value computeKey(AbstractRdfMappedStatementCollection<?> collection) {
        var value = (Value) collection.getValue();
        if (collection.getGraphs().isEmpty()) {
            return value;
        }
        return new GraphScopedMergeKey(value, collection.getGraphs());
    }

    /**
     * Type-checks {@code other} against {@code self}'s exact runtime class and returns it cast to
     * the same generic type. Throws {@link IllegalStateException} on mismatch with a descriptive
     * message — the {@code mergeMergeables()} flow groups pieces by {@link #computeKey} so cross-
     * type merges shouldn't happen, but the guard makes any future grouping bug fail fast.
     */
    @SuppressWarnings("unchecked")
    static <S extends MergeableMappingResult<Value, Statement>> S requireSameType(
            S self, MergeableMappingResult<Value, Statement> other) {
        if (other.getClass() != self.getClass()) {
            throw new IllegalStateException("Cannot merge %s with %s"
                    .formatted(self.getClass().getSimpleName(), other.getClass().getName()));
        }
        return (S) other;
    }

    /**
     * Mutates {@code self}'s element and nested-result accumulators by appending {@code other}'s
     * contents in place. Cost: O(other-size). The reference returned by {@code self.getElements()}
     * is unchanged after this call — the post-merge instance built via {@code toBuilder()} shares
     * the same mutable list so subsequent merges in the reduce chain continue to grow it.
     */
    static void mergeAccumulators(
            AbstractRdfMappedStatementCollection<?> self, AbstractRdfMappedStatementCollection<?> other) {
        self.getElements().addAll(other.getElements());
        self.getNestedResults().addAll(other.getNestedResults());
    }

    /**
     * Returns the union of two sets as an immutable {@link Set#copyOf} snapshot. Iteration order
     * of the result is unspecified ({@link Set#copyOf} contract); this matches the previous
     * inline code that the helper replaced. Dedup is by element equality.
     */
    static <E> Set<E> union(Set<E> a, Set<E> b) {
        var merged = new LinkedHashSet<>(a);
        merged.addAll(b);
        return Set.copyOf(merged);
    }

    /**
     * Builds the {@code getResults()} output for a mergeable collection: nested-result statements
     * first, then own structural statements, optionally wrapped per graph context with on-the-fly
     * blank-node remap, optionally followed by linking triples ({@code subject predicate value}
     * scoped to the same graph).
     */
    static <T extends Value> Publisher<Statement> buildResults(
            AbstractRdfMappedStatementCollection<T> collection,
            Set<Resource> linkingSubjects,
            Set<IRI> linkingPredicates) {
        if (collection.getGraphs().isEmpty()) {
            return buildDefaultGraphResults(collection, linkingSubjects, linkingPredicates);
        }
        return buildGraphScopedResults(collection, linkingSubjects, linkingPredicates);
    }

    private static <T extends Value> Flux<Statement> structuralStatementsFlux(
            AbstractRdfMappedStatementCollection<T> collection) {
        var nested = Flux.fromIterable(collection.getNestedResults()).concatMap(n -> Flux.from(n.getResults()));
        var own = Flux.defer(() -> Flux.fromStream(collection.buildOwnStatements()));
        return nested.concatWith(own);
    }

    private static <T extends Value> Publisher<Statement> buildDefaultGraphResults(
            AbstractRdfMappedStatementCollection<T> collection,
            Set<Resource> linkingSubjects,
            Set<IRI> linkingPredicates) {
        var structural = structuralStatementsFlux(collection);
        if (linkingSubjects.isEmpty() || linkingPredicates.isEmpty()) {
            return structural;
        }

        var target = (Value) collection.getValue();
        var valueFactory = SimpleValueFactory.getInstance();
        var linking = Flux.fromStream(linkingSubjects.stream()
                .flatMap(subject -> linkingPredicates.stream()
                        .map(predicate -> valueFactory.createStatement(subject, predicate, target))));
        return structural.concatWith(linking);
    }

    private static <T extends Value> Publisher<Statement> buildGraphScopedResults(
            AbstractRdfMappedStatementCollection<T> collection,
            Set<Resource> linkingSubjects,
            Set<IRI> linkingPredicates) {
        var valueFactory = SimpleValueFactory.getInstance();

        return Flux.fromIterable(collection.getGraphs()).concatMap(graph -> {
            // Per-graph blank-node remap: a per-graph map identifies fresh blank nodes for any
            // blank node that appears in the structural statements. compute-if-absent guarantees
            // the same input blank node maps to the same output blank node within the graph,
            // total cost is O(structural-size) per graph.
            Map<BNode, BNode> bnodeMap = new LinkedHashMap<>();

            var graphStructural =
                    structuralStatementsFlux(collection).map(stmt -> remapToGraph(stmt, bnodeMap, graph, valueFactory));

            if (linkingSubjects.isEmpty() || linkingPredicates.isEmpty()) {
                return graphStructural;
            }

            // Linking triples are appended after structural ones so the bnodeMap (populated by the
            // structural pass) is available for remapping the head/container reference.
            var linking = Flux.defer(() -> {
                var remappedTarget = collection.getValue() instanceof BNode targetBNode
                        ? bnodeMap.getOrDefault(targetBNode, targetBNode)
                        : (Value) collection.getValue();
                return Flux.fromStream(linkingSubjects.stream()
                        .flatMap(subject -> linkingPredicates.stream()
                                .map(predicate ->
                                        valueFactory.createStatement(subject, predicate, remappedTarget, graph))));
            });

            return graphStructural.concatWith(linking);
        });
    }

    private static Statement remapToGraph(
            Statement stmt, Map<BNode, BNode> bnodeMap, Resource graph, ValueFactory valueFactory) {
        Resource subject = stmt.getSubject() instanceof BNode bNode
                ? bnodeMap.computeIfAbsent(bNode, ignored -> valueFactory.createBNode())
                : stmt.getSubject();

        Value object = stmt.getObject() instanceof BNode bNode
                ? bnodeMap.computeIfAbsent(bNode, ignored -> valueFactory.createBNode())
                : stmt.getObject();

        return valueFactory.createStatement(subject, stmt.getPredicate(), object, graph);
    }
}
