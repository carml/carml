package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalview.DedupStrategy;
import io.carml.model.ExpressionField;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MappingResolverTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private LogicalSource logicalSource;

    @Mock
    private LogicalView logicalView;

    @Mock
    private ExpressionField expressionField;

    @Mock
    private IterableField iterableField;

    @Mock
    private ExpressionField nestedExpressionField;

    @BeforeEach
    void setUp() {
        lenient().when(logicalView.getFields()).thenReturn(Set.of());
        lenient().when(logicalView.getLeftJoins()).thenReturn(Set.of());
        lenient().when(logicalView.getInnerJoins()).thenReturn(Set.of());
        lenient().when(logicalView.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());
    }

    // --- resolve(Set) entry point ---

    @Test
    void resolve_givenEmptySet_returnsEmptyList() {
        var result = MappingResolver.resolve(Set.of());

        assertThat(result, is(empty()));
    }

    // --- Explicit view detection ---

    @Test
    void resolve_givenTriplesMapWithLogicalView_setsImplicitViewFalse() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result, hasSize(1));
        assertThat(result.get(0).isImplicitView(), is(false));
    }

    @Test
    void resolve_givenTriplesMapWithLogicalView_usesViewDirectly() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEffectiveView(), is(sameInstance(logicalView)));
    }

    @Test
    void resolve_givenTriplesMapWithLogicalView_preservesOriginalTriplesMap() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getOriginalTriplesMap(), is(sameInstance(triplesMap)));
    }

    // --- Implicit view (bare LogicalSource) ---

    @Test
    void resolve_givenTriplesMapWithBareLogicalSource_setsImplicitViewTrue() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result, hasSize(1));
        assertThat(result.get(0).isImplicitView(), is(true));
    }

    @Test
    void resolve_givenTriplesMapWithBareLogicalSource_producesNonNullEffectiveView() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEffectiveView() != null, is(true));
    }

    @Test
    void resolve_givenTriplesMapWithBareLogicalSource_preservesOriginalTriplesMap() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getOriginalTriplesMap(), is(sameInstance(triplesMap)));
    }

    // --- FieldOrigin: ExpressionField with reference ---

    @Test
    void resolve_givenExpressionFieldWithReference_usesReferenceAsOriginalExpression() {
        when(expressionField.getFieldName()).thenReturn("name");
        when(expressionField.getReference()).thenReturn("$.name");
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("name");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginalExpression(), is("$.name"));
    }

    @Test
    void resolve_givenExpressionFieldWithReference_fieldNameIsUsedAsMapKey() {
        when(expressionField.getFieldName()).thenReturn("age");
        when(expressionField.getReference()).thenReturn("$.age");
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getFieldOrigin("age").isPresent(), is(true));
        assertThat(result.get(0).getFieldOrigin("$.age").isPresent(), is(false));
    }

    // --- FieldOrigin: ExpressionField with null reference ---

    @Test
    void resolve_givenExpressionFieldWithNullReference_usesAbsoluteNameAsOriginalExpression() {
        when(expressionField.getFieldName()).thenReturn("city");
        when(expressionField.getReference()).thenReturn(null);
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("city");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginalExpression(), is("city"));
    }

    // --- FieldOrigin: non-ExpressionField (IterableField) ---

    @Test
    void resolve_givenIterableField_usesAbsoluteNameAsOriginalExpression() {
        when(iterableField.getFieldName()).thenReturn("items");
        when(iterableField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(iterableField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("items");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginalExpression(), is("items"));
    }

    // --- FieldOrigin: field is associated with the correct TriplesMap ---

    @Test
    void resolve_givenExpressionField_fieldOriginPointsToCorrectTriplesMap() {
        when(expressionField.getFieldName()).thenReturn("name");
        when(expressionField.getReference()).thenReturn("$.name");
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("name");
        assertThat(origin.get().getOriginatingTriplesMap(), is(sameInstance(triplesMap)));
    }

    // --- FieldOrigin: field object is recorded correctly ---

    @Test
    void resolve_givenExpressionField_fieldOriginRecordsFieldObject() {
        when(expressionField.getFieldName()).thenReturn("id");
        when(expressionField.getReference()).thenReturn("$.id");
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("id");
        assertThat(origin.get().getField(), is(sameInstance(expressionField)));
    }

    // --- FieldOrigin: unknown field name ---

    @Test
    void resolve_givenUnknownFieldName_returnsEmptyOrigin() {
        when(logicalView.getFields()).thenReturn(Set.of());
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getFieldOrigin("nonexistent").isPresent(), is(false));
    }

    // --- Null fields in view ---

    @Test
    void resolve_givenViewWithNullFields_doesNotThrow() {
        when(logicalView.getFields()).thenReturn(null);
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result, hasSize(1));
    }

    // --- Nested fields: dot-separated path ---

    @Test
    void resolve_givenNestedExpressionField_recordsDotSeparatedPath() {
        when(nestedExpressionField.getFieldName()).thenReturn("street");
        when(nestedExpressionField.getReference()).thenReturn("$.street");
        when(nestedExpressionField.getFields()).thenReturn(Set.of());

        when(iterableField.getFieldName()).thenReturn("address");
        when(iterableField.getFields()).thenReturn(Set.of(nestedExpressionField));
        when(logicalView.getFields()).thenReturn(Set.of(iterableField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        // Parent field recorded at "address"
        assertThat(result.get(0).getFieldOrigin("address").isPresent(), is(true));
        // Nested field recorded at "address.street"
        assertThat(result.get(0).getFieldOrigin("address.street").isPresent(), is(true));
    }

    @Test
    void resolve_givenNestedExpressionField_nestedOriginUsesReference() {
        when(nestedExpressionField.getFieldName()).thenReturn("zip");
        when(nestedExpressionField.getReference()).thenReturn("$.zip");
        when(nestedExpressionField.getFields()).thenReturn(Set.of());

        when(iterableField.getFieldName()).thenReturn("location");
        when(iterableField.getFields()).thenReturn(Set.of(nestedExpressionField));
        when(logicalView.getFields()).thenReturn(Set.of(iterableField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var nestedOrigin = result.get(0).getFieldOrigin("location.zip");
        assertThat(nestedOrigin.isPresent(), is(true));
        assertThat(nestedOrigin.get().getOriginalExpression(), is("$.zip"));
    }

    @Test
    void resolve_givenNestedExpressionField_parentOriginUsesAbsoluteName() {
        when(nestedExpressionField.getFieldName()).thenReturn("zip");
        when(nestedExpressionField.getReference()).thenReturn("$.zip");
        when(nestedExpressionField.getFields()).thenReturn(Set.of());

        when(iterableField.getFieldName()).thenReturn("location");
        when(iterableField.getFields()).thenReturn(Set.of(nestedExpressionField));
        when(logicalView.getFields()).thenReturn(Set.of(iterableField));
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);

        var result = MappingResolver.resolve(Set.of(triplesMap));

        // IterableField is not an ExpressionField, so it falls back to absoluteName
        var parentOrigin = result.get(0).getFieldOrigin("location");
        assertThat(parentOrigin.isPresent(), is(true));
        assertThat(parentOrigin.get().getOriginalExpression(), is("location"));
    }

    // --- EvaluationContext: explicit view ---

    @Test
    void resolve_givenExplicitView_evaluationContextIsNonNull() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext(), is(notNullValue()));
    }

    @Test
    void resolve_givenExplicitView_projectedFieldsMatchReferenceExpressionSet() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name", "age"));

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getProjectedFields(), is(Set.of("name", "age")));
    }

    @Test
    void resolve_givenExplicitViewWithNoReferences_projectedFieldsAreEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getProjectedFields(), is(empty()));
    }

    @Test
    void resolve_givenExplicitView_dedupStrategyIsNone() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(
                result.get(0).getEvaluationContext().getDedupStrategy(),
                is(instanceOf(DedupStrategy.none().getClass())));
    }

    @Test
    void resolve_givenExplicitView_limitIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getLimit().isPresent(), is(false));
    }

    @Test
    void resolve_givenExplicitView_joinWindowDurationIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getJoinWindowDuration().isPresent(), is(false));
    }

    @Test
    void resolve_givenExplicitView_joinWindowCountIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getJoinWindowCount().isPresent(), is(false));
    }

    // --- EvaluationContext: implicit view ---

    @Test
    void resolve_givenImplicitView_evaluationContextIsNonNull() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext(), is(notNullValue()));
    }

    @Test
    void resolve_givenImplicitView_projectedFieldsAreEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.name", "$.age"));

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getProjectedFields(), is(empty()));
    }

    @Test
    void resolve_givenImplicitView_dedupStrategyIsNone() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(
                result.get(0).getEvaluationContext().getDedupStrategy(),
                is(instanceOf(DedupStrategy.none().getClass())));
    }

    @Test
    void resolve_givenImplicitView_limitIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getLimit().isPresent(), is(false));
    }

    @Test
    void resolve_givenImplicitView_joinWindowDurationIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getJoinWindowDuration().isPresent(), is(false));
    }

    @Test
    void resolve_givenImplicitView_joinWindowCountIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap));

        assertThat(result.get(0).getEvaluationContext().getJoinWindowCount().isPresent(), is(false));
    }

    // --- FieldOrigin: TermMap association for implicit views ---

    @Test
    void resolve_givenImplicitViewWithSubjectMapReference_fieldOriginCarriesSubjectMapAsTermMap() {
        var subjectMap = mock(SubjectMap.class);
        when(subjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("$.name"));
        when(subjectMap.getGraphMaps()).thenReturn(Set.of());

        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of());
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.name"));

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("$.name");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginatingTermMap().isPresent(), is(true));
        assertThat(origin.get().getOriginatingTermMap().get(), is(sameInstance(subjectMap)));
    }

    @Test
    void resolve_givenImplicitViewWithObjectMapReference_fieldOriginCarriesObjectMapAsTermMap() {
        var subjectMap = mock(SubjectMap.class);
        when(subjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of());
        when(subjectMap.getGraphMaps()).thenReturn(Set.of());

        var objectMap = mock(ObjectMap.class);
        when(objectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("$.age"));

        var pom = mock(PredicateObjectMap.class);
        when(pom.getPredicateMaps()).thenReturn(Set.of());
        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap));
        when(pom.getGraphMaps()).thenReturn(Set.of());

        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));
        when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("$.age"));

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("$.age");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginatingTermMap().isPresent(), is(true));
        assertThat(origin.get().getOriginatingTermMap().get(), is(sameInstance(objectMap)));
    }

    @Test
    void resolve_givenExplicitView_fieldOriginHasNoTermMap() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(expressionField.getFieldName()).thenReturn("name");
        when(expressionField.getReference()).thenReturn("$.name");
        when(expressionField.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(expressionField));
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name"));

        var result = MappingResolver.resolve(Set.of(triplesMap));

        var origin = result.get(0).getFieldOrigin("name");
        assertThat(origin.isPresent(), is(true));
        assertThat(origin.get().getOriginatingTermMap().isPresent(), is(false));
    }
}
