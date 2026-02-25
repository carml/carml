package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
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
}
