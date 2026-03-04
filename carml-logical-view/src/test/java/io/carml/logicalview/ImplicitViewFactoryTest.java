package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.ChildMap;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.ParentMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
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
        lenient().when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of());
    }

    @Test
    void wrap_givenTriplesMapWithReferenceFields_thenFieldsMatchExpressions() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.name", "$.age"));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var logicalView = wrapResult.view();

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
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var logicalView = wrapResult.view();

        // Then
        assertThat(logicalView.getFields(), is(empty()));
    }

    @Test
    void wrap_givenTriplesMapWithTemplateExpressions_thenFieldsCreated() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name", "city"));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var logicalView = wrapResult.view();

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
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var logicalView = wrapResult.view();

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
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var logicalView = wrapResult.view();

        // Then
        for (Field field : logicalView.getFields()) {
            assertThat(field, is(instanceOf(ExpressionField.class)));
            var expressionField = (ExpressionField) field;
            assertThat(expressionField.getReference(), is(expressionField.getFieldName()));
            assertThat(field.getFields(), is(empty()));
        }
    }

    @Test
    void wrap_givenTriplesMapWithNoJoiningRefObjectMaps_thenEmptyPrefixes() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.id"));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);

        // Then
        assertThat(wrapResult.refObjectMapPrefixes().entrySet(), is(empty()));
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

    @Test
    void wrap_givenTriplesMapWithOneJoiningRefObjectMap_thenCreatesLeftJoinAndPrefix() {
        // Given - child TriplesMap with a reference expression
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("childExpr"));

        // Parent TriplesMap with its own LogicalSource and SubjectMap
        var parentLogicalSource = mock(LogicalSource.class);
        var parentTriplesMap = mock(TriplesMap.class);
        lenient().when(parentTriplesMap.getLogicalSource()).thenReturn(parentLogicalSource);
        lenient().when(parentTriplesMap.getResourceName()).thenReturn("ParentTM");

        var parentSubjectMap = mock(SubjectMap.class);
        when(parentSubjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("parentExpr"));
        when(parentTriplesMap.getSubjectMaps()).thenReturn(Set.of(parentSubjectMap));

        // Join condition: childMap("childKey") = parentMap("parentKey")
        var childMap = mock(ChildMap.class);
        lenient().when(childMap.getExpressionMapExpressionSet()).thenReturn(Set.of("childKey"));
        var parentMap = mock(ParentMap.class);
        when(parentMap.getExpressionMapExpressionSet()).thenReturn(Set.of("parentKey"));
        var join = mock(Join.class);
        lenient().when(join.getChildMap()).thenReturn(childMap);
        when(join.getParentMap()).thenReturn(parentMap);

        // RefObjectMap: non-empty join conditions, not self-joining (different logical sources)
        var rom = mock(RefObjectMap.class);
        when(rom.getParentTriplesMap()).thenReturn(parentTriplesMap);
        when(rom.getJoinConditions()).thenReturn(Set.of(join));
        when(rom.isSelfJoining(triplesMap)).thenReturn(false);

        // PredicateObjectMap containing the RefObjectMap
        var pom = mock(PredicateObjectMap.class);
        when(pom.getObjectMaps()).thenReturn(Set.of(rom));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var view = wrapResult.view();

        // Then - left join created
        assertThat(view.getLeftJoins(), hasSize(1));

        // Then - prefix assigned
        assertThat(wrapResult.refObjectMapPrefixes(), hasEntry(rom, "_ref0."));

        // Then - left join fields have prefixed field names
        var leftJoin = view.getLeftJoins().iterator().next();
        var joinFieldNames =
                leftJoin.getFields().stream().map(Field::getFieldName).collect(Collectors.toUnmodifiableSet());
        assertThat(joinFieldNames, is(Set.of("_ref0.parentExpr")));

        // Then - parent logical view has fields for parent SubjectMap + join parent expressions
        var parentViewFields = leftJoin.getParentLogicalView().getFields().stream()
                .map(Field::getFieldName)
                .collect(Collectors.toUnmodifiableSet());
        assertThat(parentViewFields, is(Set.of("parentExpr", "parentKey")));

        // Then - join conditions are the ROM's join conditions
        assertThat(leftJoin.getJoinConditions(), is(Set.of(join)));
    }

    @Test
    void wrap_givenTwoJoiningRefObjectMaps_thenPrefixesAssignedDeterministically() {
        // Given
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("childExpr"));

        // Parent TriplesMap A (resource name "A")
        var parentLogicalSourceA = mock(LogicalSource.class);
        var parentTriplesMapA = mock(TriplesMap.class);
        lenient().when(parentTriplesMapA.getLogicalSource()).thenReturn(parentLogicalSourceA);
        when(parentTriplesMapA.getResourceName()).thenReturn("A");
        var parentSubjectMapA = mock(SubjectMap.class);
        when(parentSubjectMapA.getExpressionMapExpressionSet()).thenReturn(Set.of("exprA"));
        when(parentTriplesMapA.getSubjectMaps()).thenReturn(Set.of(parentSubjectMapA));

        // Parent TriplesMap B (resource name "B")
        var parentLogicalSourceB = mock(LogicalSource.class);
        var parentTriplesMapB = mock(TriplesMap.class);
        lenient().when(parentTriplesMapB.getLogicalSource()).thenReturn(parentLogicalSourceB);
        when(parentTriplesMapB.getResourceName()).thenReturn("B");
        var parentSubjectMapB = mock(SubjectMap.class);
        when(parentSubjectMapB.getExpressionMapExpressionSet()).thenReturn(Set.of("exprB"));
        when(parentTriplesMapB.getSubjectMaps()).thenReturn(Set.of(parentSubjectMapB));

        // Join conditions for ROM A
        var childMapA = mock(ChildMap.class);
        lenient().when(childMapA.getExpressionMapExpressionSet()).thenReturn(Set.of("ckA"));
        var parentMapA = mock(ParentMap.class);
        when(parentMapA.getExpressionMapExpressionSet()).thenReturn(Set.of("pkA"));
        var joinA = mock(Join.class);
        lenient().when(joinA.getChildMap()).thenReturn(childMapA);
        when(joinA.getParentMap()).thenReturn(parentMapA);

        // Join conditions for ROM B
        var childMapB = mock(ChildMap.class);
        lenient().when(childMapB.getExpressionMapExpressionSet()).thenReturn(Set.of("ckB"));
        var parentMapB = mock(ParentMap.class);
        when(parentMapB.getExpressionMapExpressionSet()).thenReturn(Set.of("pkB"));
        var joinB = mock(Join.class);
        lenient().when(joinB.getChildMap()).thenReturn(childMapB);
        when(joinB.getParentMap()).thenReturn(parentMapB);

        // RefObjectMap pointing to parent A
        var romA = mock(RefObjectMap.class);
        when(romA.getParentTriplesMap()).thenReturn(parentTriplesMapA);
        when(romA.getJoinConditions()).thenReturn(Set.of(joinA));
        when(romA.isSelfJoining(triplesMap)).thenReturn(false);

        // RefObjectMap pointing to parent B
        var romB = mock(RefObjectMap.class);
        when(romB.getParentTriplesMap()).thenReturn(parentTriplesMapB);
        when(romB.getJoinConditions()).thenReturn(Set.of(joinB));
        when(romB.isSelfJoining(triplesMap)).thenReturn(false);

        // Two POMs, each with one ROM
        var pomA = mock(PredicateObjectMap.class);
        when(pomA.getObjectMaps()).thenReturn(Set.of(romA));
        var pomB = mock(PredicateObjectMap.class);
        when(pomB.getObjectMaps()).thenReturn(Set.of(romB));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pomA, pomB));

        // When - call wrap twice
        var result1 = ImplicitViewFactory.wrap(triplesMap);
        var result2 = ImplicitViewFactory.wrap(triplesMap);

        // Then - ROM pointing to parent "A" gets _ref0., ROM pointing to "B" gets _ref1. (sorted by
        // resourceName)
        assertThat(result1.refObjectMapPrefixes(), hasEntry(romA, "_ref0."));
        assertThat(result1.refObjectMapPrefixes(), hasEntry(romB, "_ref1."));

        // Then - second call yields the same deterministic assignment
        assertThat(result2.refObjectMapPrefixes(), hasEntry(romA, "_ref0."));
        assertThat(result2.refObjectMapPrefixes(), hasEntry(romB, "_ref1."));
    }

    @Test
    void wrap_givenSelfJoiningRefObjectMap_thenNoLeftJoinCreated() {
        // Given - set up a self-joining scenario: same logical source, different triples maps,
        // matching child/parent expressions
        var parentTriplesMap = mock(TriplesMap.class);
        lenient().when(parentTriplesMap.getLogicalSource()).thenReturn(logicalSource); // same logical source

        var parentSubjectMap = mock(SubjectMap.class);
        when(parentSubjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("parentId"));
        when(parentTriplesMap.getSubjectMaps()).thenReturn(Set.of(parentSubjectMap));

        // Join condition where child and parent expressions match (self-joining)
        var childMap = mock(ChildMap.class);
        lenient().when(childMap.getExpressionMapExpressionSet()).thenReturn(Set.of("id"));
        var parentMap = mock(ParentMap.class);
        lenient().when(parentMap.getExpressionMapExpressionSet()).thenReturn(Set.of("id"));
        var join = mock(Join.class);
        lenient().when(join.getChildMap()).thenReturn(childMap);
        lenient().when(join.getParentMap()).thenReturn(parentMap);

        var rom = mock(RefObjectMap.class);
        when(rom.getParentTriplesMap()).thenReturn(parentTriplesMap);
        when(rom.getJoinConditions()).thenReturn(Set.of(join));
        when(rom.isSelfJoining(triplesMap)).thenReturn(true);

        var pom = mock(PredicateObjectMap.class);
        when(pom.getObjectMaps()).thenReturn(Set.of(rom));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("childExpr"));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var view = wrapResult.view();

        // Then - no left joins created for self-joining ROM
        assertThat(view.getLeftJoins(), is(empty()));

        // Then - no prefixes assigned
        assertThat(wrapResult.refObjectMapPrefixes().entrySet(), is(empty()));

        // Then - parent SubjectMap expressions ARE added to the view fields (joinless parent
        // expressions)
        var fieldNames = view.getFields().stream().map(Field::getFieldName).collect(Collectors.toUnmodifiableSet());
        assertThat(fieldNames.contains("parentId"), is(true));
    }

    @Test
    void wrap_givenJoinlessRefObjectMap_thenParentExpressionsAddedToFields() {
        // Given - RefObjectMap with no join conditions
        var parentTriplesMap = mock(TriplesMap.class);
        lenient().when(parentTriplesMap.getLogicalSource()).thenReturn(logicalSource);

        var parentSubjectMap = mock(SubjectMap.class);
        when(parentSubjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("parentId"));
        when(parentTriplesMap.getSubjectMaps()).thenReturn(Set.of(parentSubjectMap));

        var rom = mock(RefObjectMap.class);
        when(rom.getParentTriplesMap()).thenReturn(parentTriplesMap);
        when(rom.getJoinConditions()).thenReturn(Set.of());
        lenient().when(rom.isSelfJoining(triplesMap)).thenReturn(false);

        var pom = mock(PredicateObjectMap.class);
        when(pom.getObjectMaps()).thenReturn(Set.of(rom));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("childExpr"));

        // When
        var wrapResult = ImplicitViewFactory.wrap(triplesMap);
        var view = wrapResult.view();

        // Then - no left joins since there are no join conditions
        assertThat(view.getLeftJoins(), is(empty()));

        // Then - parent SubjectMap expressions included in the view fields
        var fieldNames = view.getFields().stream().map(Field::getFieldName).collect(Collectors.toUnmodifiableSet());
        assertThat(fieldNames, is(Set.of("childExpr", "parentId")));
    }
}
