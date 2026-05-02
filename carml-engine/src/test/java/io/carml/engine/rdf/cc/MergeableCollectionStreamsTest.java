package io.carml.engine.rdf.cc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

/**
 * Documentation-style tests for the non-obvious contract decisions in
 * {@link MergeableCollectionStreams}. The behavioral surface is fully covered by the
 * {@code MergeableRdfList} / {@code MergeableRdfContainer} test classes; these tests target
 * specific contract points (union order/dedup semantics; exact-class type check vs. instanceof
 * semantics) where the documentation value is greater than the indirect coverage.
 */
class MergeableCollectionStreamsTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    void union_combinesAndDedupes_returningImmutableSet() {
        // Documents the contract: result is the set union (dedup by equality) and is immutable.
        // Iteration order of the result is unspecified — the helper uses Set.copyOf, matching
        // the pre-refactor inline code. Don't add an order assertion here without also changing
        // the helper to use Collections.unmodifiableSet on the LinkedHashSet.
        var first = Set.of("a", "b");
        var second = Set.of("c", "a", "d");

        var result = MergeableCollectionStreams.union(first, second);

        assertThat(result, is(Set.of("a", "b", "c", "d")));
        assertThrows(UnsupportedOperationException.class, () -> result.add("e"));
    }

    @Test
    void requireSameType_rejectsSubclasses_eventThoughInstanceofWouldPass() {
        // The helper uses other.getClass() != self.getClass() — an exact-class check, NOT
        // pattern-matching instanceof. A subclass of MergeableRdfList passes
        // `other instanceof MergeableRdfList` but is rejected by the helper, because shared
        // accumulator semantics depend on identical runtime types on both sides of the merge.
        var parent = MergeableRdfList.<Value>builder().head(VF.createBNode("p")).build();
        var child = TestSubMergeableRdfList.<Value>builder()
                .head(VF.createBNode("c"))
                .build();

        assertThat(
                "Test fixture sanity: subclass IS a MergeableRdfList by instanceof",
                child,
                instanceOf(MergeableRdfList.class));

        var thrown = assertThrows(
                IllegalStateException.class, () -> MergeableCollectionStreams.requireSameType(parent, child));
        assertThat(thrown.getMessage(), containsString("MergeableRdfList"));
        assertThat(thrown.getMessage(), containsString("TestSubMergeableRdfList"));
    }

    @SuperBuilder(toBuilder = true)
    private static class TestSubMergeableRdfList<T extends Value> extends MergeableRdfList<T> {}
}
