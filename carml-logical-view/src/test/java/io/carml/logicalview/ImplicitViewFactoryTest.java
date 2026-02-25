package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.model.TriplesMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ImplicitViewFactoryTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private LogicalSource logicalSource;

    @BeforeEach
    void setUp() {
        lenient().when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
    }

    @Test
    void wrap_givenTriplesMapWithReferenceFields_thenFieldsMatchExpressions() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.name", "$.age"));

        // When
        var logicalView = ImplicitViewFactory.wrap(triplesMap);

        // Then
        assertThat(logicalView.getViewOn(), is(sameInstance(logicalSource)));
        assertThat(logicalView.getFields(), hasSize(2));

        var fieldNames =
                logicalView.getFields().stream().map(Field::getFieldName).collect(Collectors.toUnmodifiableSet());

        assertThat(fieldNames, is(Set.of("$.name", "$.age")));
    }

    @Test
    void wrap_givenTriplesMapWithNoReferences_thenEmptyFields() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        // When
        var logicalView = ImplicitViewFactory.wrap(triplesMap);

        // Then
        assertThat(logicalView.getFields(), is(empty()));
    }

    @Test
    void wrap_givenTriplesMapWithTemplateExpressions_thenFieldsCreated() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name", "city"));

        // When
        var logicalView = ImplicitViewFactory.wrap(triplesMap);

        // Then
        assertThat(logicalView.getFields(), hasSize(2));

        var fieldNames =
                logicalView.getFields().stream().map(Field::getFieldName).collect(Collectors.toUnmodifiableSet());

        assertThat(fieldNames, is(Set.of("name", "city")));
    }

    @Test
    void wrap_givenTriplesMap_thenNoJoinsOrAnnotations() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.id"));

        // When
        var logicalView = ImplicitViewFactory.wrap(triplesMap);

        // Then
        assertThat(logicalView.getLeftJoins(), is(empty()));
        assertThat(logicalView.getInnerJoins(), is(empty()));
        assertThat(logicalView.getStructuralAnnotations(), is(empty()));
    }

    @Test
    void wrap_givenTriplesMap_thenFieldsAreExpressionFieldsWithMatchingReferenceAndFieldName() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.name", "$.age"));

        // When
        var logicalView = ImplicitViewFactory.wrap(triplesMap);

        // Then
        for (Field field : logicalView.getFields()) {
            assertThat(field, is(instanceOf(ExpressionField.class)));
            var expressionField = (ExpressionField) field;
            assertThat(expressionField.getReference(), is(expressionField.getFieldName()));
            assertThat(field.getFields(), is(empty()));
        }
    }

    @Test
    void wrap_givenNullTriplesMap_thenThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> ImplicitViewFactory.wrap(null));
    }

    @Test
    void wrap_givenNullLogicalSource_thenThrowsNullPointerException() {
        when(triplesMap.getLogicalSource()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> ImplicitViewFactory.wrap(triplesMap));
    }
}
