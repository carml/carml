package io.carml.engine.rdf.cc;

import io.carml.engine.MergeableMappingResult;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.reactivestreams.Publisher;

/**
 * Mergeable variant of {@link RdfList}. Multiple pieces produced by per-iteration evaluation are
 * combined via {@link #merge} into a single accumulated list. Merging mutates the
 * {@code elements} and {@code nestedResults} accumulators of {@code this} in place; the returned
 * builder-built instance shares those mutable references, so subsequent merges in the same
 * {@link java.util.stream.Stream#reduce} chain continue to grow the same accumulator. Total cost
 * over N pieces of size k is O(N·k) rather than the O(N²·k) of the previous Model-rebuild design.
 *
 * <p><b>Mutation contract:</b> {@link #merge} mutates {@code this}'s {@code elements} and
 * {@code nestedResults} lists. Callers of {@code merge} must not retain a reference to either
 * input after merging — the post-merge accumulator subsumes both.
 *
 * <p>The streaming/merge logic shared with {@link MergeableRdfContainer} lives in
 * {@link MergeableCollectionStreams}.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
public class MergeableRdfList<T extends Value> extends RdfList<T> implements MergeableMappingResult<Value, Statement> {

    /**
     * Subjects for generating linking triples ({@code subject predicate head graph}).
     */
    @Builder.Default
    private final Set<Resource> linkingSubjects = Set.of();

    /**
     * Predicates for generating linking triples ({@code subject predicate head graph}).
     */
    @Builder.Default
    private final Set<IRI> linkingPredicates = Set.of();

    @Override
    public Value getKey() {
        return MergeableCollectionStreams.computeKey(this);
    }

    @Override
    public MergeableMappingResult<Value, Statement> merge(MergeableMappingResult<Value, Statement> other) {
        var otherList = MergeableCollectionStreams.requireSameType(this, other);

        // Mutate the shared accumulator lists in place. Cost: O(other.size()).
        MergeableCollectionStreams.mergeAccumulators(this, otherList);

        // The returned builder pre-fills with this instance's mutable element/nested lists, so
        // subsequent merges in the reduce chain continue to grow the same accumulator. The
        // .logicalTargets(...) call adds (under @Singular semantics) the other side's targets to
        // the pre-filled this-side targets, producing the union.
        return toBuilder()
                .logicalTargets(otherList.getLogicalTargets())
                .linkingSubjects(MergeableCollectionStreams.union(linkingSubjects, otherList.getLinkingSubjects()))
                .linkingPredicates(
                        MergeableCollectionStreams.union(linkingPredicates, otherList.getLinkingPredicates()))
                .build();
    }

    @Override
    public Publisher<Statement> getResults() {
        return MergeableCollectionStreams.buildResults(this, linkingSubjects, linkingPredicates);
    }

    /**
     * Creates a new {@link MergeableRdfList} scoped to the given graphs with linking triple info.
     * Graph scoping with fresh blank nodes is applied when {@link #getResults()} is called.
     */
    public MergeableRdfList<T> withGraphScope(Set<Resource> targetGraphs, Set<Resource> subjects, Set<IRI> predicates) {
        return toBuilder()
                .graphs(targetGraphs)
                .linkingSubjects(subjects)
                .linkingPredicates(predicates)
                .build();
    }
}
