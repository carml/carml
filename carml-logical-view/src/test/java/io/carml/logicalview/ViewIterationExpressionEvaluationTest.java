package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ViewIterationExpressionEvaluationTest {

    @Test
    void apply_givenReferenceableKey_thenReturnsValue() {
        var iteration = mock(ViewIteration.class);
        when(iteration.getValue("name")).thenReturn(Optional.of("alice"));

        var referenceableKeys = Set.of("#", "name", "name.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        assertThat(evaluation.apply("name"), is(Optional.of("alice")));
    }

    @Test
    void apply_givenRootIndexKey_thenReturnsValue() {
        var iteration = mock(ViewIteration.class);
        when(iteration.getValue("#")).thenReturn(Optional.of(0));

        var referenceableKeys = Set.of("#", "name", "name.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        assertThat(evaluation.apply("#"), is(Optional.of(0)));
    }

    @Test
    void apply_givenFieldIndexKey_thenReturnsValue() {
        var iteration = mock(ViewIteration.class);
        when(iteration.getValue("name.#")).thenReturn(Optional.of(2));

        var referenceableKeys = Set.of("#", "name", "name.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        assertThat(evaluation.apply("name.#"), is(Optional.of(2)));
    }

    @Test
    void apply_givenNonExistingKey_thenThrowsException() {
        var iteration = mock(ViewIteration.class);
        var referenceableKeys = Set.of("#", "name", "name.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        var exception =
                assertThrows(ViewIterationExpressionEvaluationException.class, () -> evaluation.apply("nonexistent"));

        assertThat(exception.getMessage(), containsString("Reference to non-existing key 'nonexistent'"));
        assertThat(exception.getMessage(), containsString("does not exist in the logical view"));
        assertThat(exception.getMessage(), containsString("Available keys:"));
    }

    @Test
    void apply_givenItKey_thenThrowsException() {
        var iteration = mock(ViewIteration.class);
        var referenceableKeys = Set.of("#", "name", "name.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        var exception = assertThrows(ViewIterationExpressionEvaluationException.class, () -> evaluation.apply("<it>"));

        assertThat(exception.getMessage(), containsString("Reference to root iterable record key '<it>'"));
        assertThat(exception.getMessage(), containsString("is not a referenceable key in a logical view"));
    }

    @Test
    void apply_givenIterableRecordKey_thenThrowsException() {
        var iteration = mock(ViewIteration.class);
        // "items.#" is referenceable (iterable index key), but "items" itself is not (iterable record key)
        var referenceableKeys = Set.of("#", "items.#", "items.type", "items.type.#");
        var evaluation = new ViewIterationExpressionEvaluation(iteration, referenceableKeys);

        var exception = assertThrows(ViewIterationExpressionEvaluationException.class, () -> evaluation.apply("items"));

        assertThat(exception.getMessage(), containsString("Reference to iterable record key 'items'"));
        assertThat(exception.getMessage(), containsString("is not a referenceable key"));
        assertThat(exception.getMessage(), containsString("Use its nested field names instead"));
    }
}
