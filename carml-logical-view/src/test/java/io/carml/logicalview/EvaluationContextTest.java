package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EvaluationContextTest {

    @Test
    void withProjectedFields_givenNull_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> EvaluationContext.withProjectedFields(null));
    }

    @Test
    void withProjectedFields_givenMutableSet_doesNotReflectSubsequentMutation() {
        var mutable = new HashSet<>(Set.of("a", "b"));
        var ctx = EvaluationContext.withProjectedFields(mutable);
        mutable.add("c");
        assertThat(ctx.getProjectedFields(), is(Set.of("a", "b")));
    }

    @Test
    void withProjectedFields_givenNonEmptySet_projectedFieldsMatch() {
        var ctx = EvaluationContext.withProjectedFields(Set.of("name", "age"));
        assertThat(ctx.getProjectedFields(), is(Set.of("name", "age")));
    }

    @Test
    void withProjectedFields_givenEmptySet_projectedFieldsAreEmpty() {
        var ctx = EvaluationContext.withProjectedFields(Set.of());
        assertThat(ctx.getProjectedFields().isEmpty(), is(true));
    }

    // --- withProjectedFieldsAndLimit ---

    @Test
    void withProjectedFieldsAndLimit_givenNullProjectedFields_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> EvaluationContext.withProjectedFieldsAndLimit(null, 10L));
    }

    @Test
    void withProjectedFieldsAndLimit_givenNullLimit_limitIsEmpty() {
        var ctx = EvaluationContext.withProjectedFieldsAndLimit(Set.of("a"), null);
        assertThat(ctx.getLimit(), is(Optional.empty()));
    }

    @Test
    void withProjectedFieldsAndLimit_givenPositiveLimit_limitIsPresent() {
        var ctx = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 42L);
        assertThat(ctx.getLimit(), is(Optional.of(42L)));
    }

    @Test
    void withProjectedFieldsAndLimit_givenZeroLimit_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 0L));
    }

    @Test
    void withProjectedFieldsAndLimit_givenNegativeLimit_throwsIllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class, () -> EvaluationContext.withProjectedFieldsAndLimit(Set.of(), -5L));
    }

    @Test
    void withProjectedFieldsAndLimit_givenFields_projectedFieldsMatch() {
        var ctx = EvaluationContext.withProjectedFieldsAndLimit(Set.of("x", "y"), 10L);
        assertThat(ctx.getProjectedFields(), is(Set.of("x", "y")));
    }

    @Test
    void withProjectedFieldsAndLimit_givenMutableSet_doesNotReflectSubsequentMutation() {
        var mutable = new HashSet<>(Set.of("a", "b"));
        var ctx = EvaluationContext.withProjectedFieldsAndLimit(mutable, 5L);
        mutable.add("c");
        assertThat(ctx.getProjectedFields(), is(Set.of("a", "b")));
    }
}
