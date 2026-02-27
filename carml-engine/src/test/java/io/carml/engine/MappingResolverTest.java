package io.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalview.DedupStrategy;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IriSafeAnnotation;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.UniqueAnnotation;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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

    // --- Limit parameter flow ---

    @Test
    void resolve_givenExplicitViewWithLimit_evaluationContextContainsLimit() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap), 100L);

        assertThat(result.get(0).getEvaluationContext().getLimit(), is(Optional.of(100L)));
    }

    @Test
    void resolve_givenExplicitViewWithNullLimit_evaluationContextLimitIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap), null);

        assertThat(result.get(0).getEvaluationContext().getLimit().isPresent(), is(false));
    }

    @Test
    void resolve_givenImplicitViewWithLimit_evaluationContextContainsLimit() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap), 50L);

        assertThat(result.get(0).getEvaluationContext().getLimit(), is(Optional.of(50L)));
    }

    @Test
    void resolve_givenImplicitViewWithNullLimit_evaluationContextLimitIsEmpty() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var result = MappingResolver.resolve(Set.of(triplesMap), null);

        assertThat(result.get(0).getEvaluationContext().getLimit().isPresent(), is(false));
    }

    @Test
    void resolve_noArgOverload_delegatesToResolveWithNullLimit() {
        when(triplesMap.getLogicalSource()).thenReturn(logicalView);
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of());

        var triplesMaps = Set.of(triplesMap);
        var noArgResult = MappingResolver.resolve(triplesMaps);
        var nullLimitResult = MappingResolver.resolve(triplesMaps, null);

        assertThat(noArgResult.size(), is(nullLimitResult.size()));
        assertThat(
                noArgResult.get(0).getEvaluationContext().getLimit(),
                is(nullLimitResult.get(0).getEvaluationContext().getLimit()));
    }

    // --- Cycle detection: viewOn cycles ---

    @Test
    void validateNoCycles_givenViewOnCycle_throwsRuntimeException() {
        var viewA = mock(LogicalView.class);
        var viewB = mock(LogicalView.class);

        when(viewA.getResourceName()).thenReturn("viewA");
        when(viewA.getViewOn()).thenReturn(viewB);
        lenient().when(viewA.getLeftJoins()).thenReturn(Set.of());
        lenient().when(viewA.getInnerJoins()).thenReturn(Set.of());
        lenient().when(viewA.getFields()).thenReturn(Set.of());

        when(viewB.getResourceName()).thenReturn("viewB");
        when(viewB.getViewOn()).thenReturn(viewA);
        lenient().when(viewB.getLeftJoins()).thenReturn(Set.of());
        lenient().when(viewB.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoCycles(viewA));

        assertThat(exception.getMessage(), containsString("Cycle detected in logical view structure"));
        assertThat(exception.getMessage(), containsString("viewA"));
        assertThat(exception.getMessage(), containsString("viewB"));
    }

    // --- Cycle detection: join cycles ---

    @Test
    void validateNoCycles_givenJoinCycle_throwsRuntimeException() {
        var viewA = mock(LogicalView.class);
        var viewB = mock(LogicalView.class);
        var joinAtoB = mock(LogicalViewJoin.class);
        var joinBtoA = mock(LogicalViewJoin.class);

        when(viewA.getResourceName()).thenReturn("viewA");
        when(viewA.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(viewA.getLeftJoins()).thenReturn(Set.of(joinAtoB));
        when(viewA.getInnerJoins()).thenReturn(Set.of());
        lenient().when(viewA.getFields()).thenReturn(Set.of());

        when(joinAtoB.getParentLogicalView()).thenReturn(viewB);

        when(viewB.getResourceName()).thenReturn("viewB");
        when(viewB.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(viewB.getLeftJoins()).thenReturn(Set.of(joinBtoA));
        lenient().when(viewB.getInnerJoins()).thenReturn(Set.of());

        when(joinBtoA.getParentLogicalView()).thenReturn(viewA);

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoCycles(viewA));

        assertThat(exception.getMessage(), containsString("Cycle detected in logical view structure"));
        assertThat(exception.getMessage(), containsString("viewA"));
        assertThat(exception.getMessage(), containsString("viewB"));
    }

    // --- Cycle detection: self-join cycles ---

    @Test
    void validateNoCycles_givenSelfJoin_throwsRuntimeException() {
        var view = mock(LogicalView.class);
        var selfJoin = mock(LogicalViewJoin.class);

        when(view.getResourceName()).thenReturn("selfView");
        when(view.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(view.getLeftJoins()).thenReturn(Set.of(selfJoin));
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());
        lenient().when(view.getFields()).thenReturn(Set.of());

        when(selfJoin.getParentLogicalView()).thenReturn(view);

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoCycles(view));

        assertThat(exception.getMessage(), containsString("Cycle detected in logical view structure"));
        assertThat(exception.getMessage(), containsString("selfView"));
    }

    // --- Cycle detection: field cycles ---

    @Test
    void validateNoCycles_givenFieldCycle_throwsRuntimeException() {
        var view = mock(LogicalView.class);
        var field1 = mock(Field.class);
        var field2 = mock(Field.class);

        when(view.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(field1));

        when(field1.getFieldName()).thenReturn("field1");
        when(field1.getFields()).thenReturn(Set.of(field2));

        when(field2.getFieldName()).thenReturn("field2");
        when(field2.getFields()).thenReturn(Set.of(field1));

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoCycles(view));

        assertThat(exception.getMessage(), containsString("Cycle detected in field tree"));
        assertThat(exception.getMessage(), containsString("field1"));
        assertThat(exception.getMessage(), containsString("field2"));
    }

    // --- Cycle detection: valid view (no cycle) ---

    @Test
    void validateNoCycles_givenValidView_doesNotThrow() {
        var view = mock(LogicalView.class);
        var field = mock(Field.class);

        when(view.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(field));

        when(field.getFieldName()).thenReturn("name");
        when(field.getFields()).thenReturn(Set.of());

        assertDoesNotThrow(() -> MappingResolver.validateNoCycles(view));
    }

    // --- Cycle detection: deep chain without cycle ---

    @Test
    void validateNoCycles_givenDeepViewOnChainWithoutCycle_doesNotThrow() {
        var viewA = mock(LogicalView.class);
        var viewB = mock(LogicalView.class);
        var viewC = mock(LogicalView.class);

        when(viewA.getResourceName()).thenReturn("viewA");
        when(viewA.getViewOn()).thenReturn(viewB);
        when(viewA.getLeftJoins()).thenReturn(Set.of());
        when(viewA.getInnerJoins()).thenReturn(Set.of());
        when(viewA.getFields()).thenReturn(Set.of());

        when(viewB.getResourceName()).thenReturn("viewB");
        when(viewB.getViewOn()).thenReturn(viewC);
        when(viewB.getLeftJoins()).thenReturn(Set.of());
        when(viewB.getInnerJoins()).thenReturn(Set.of());

        when(viewC.getResourceName()).thenReturn("viewC");
        when(viewC.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(viewC.getLeftJoins()).thenReturn(Set.of());
        when(viewC.getInnerJoins()).thenReturn(Set.of());

        assertDoesNotThrow(() -> MappingResolver.validateNoCycles(viewA));
    }

    @Test
    void validateNoCycles_givenDeepFieldTreeWithoutCycle_doesNotThrow() {
        var view = mock(LogicalView.class);
        var parent = mock(Field.class);
        var child = mock(Field.class);
        var grandchild = mock(Field.class);

        when(view.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(parent));

        when(parent.getFieldName()).thenReturn("parent");
        when(parent.getFields()).thenReturn(Set.of(child));

        when(child.getFieldName()).thenReturn("child");
        when(child.getFields()).thenReturn(Set.of(grandchild));

        when(grandchild.getFieldName()).thenReturn("grandchild");
        when(grandchild.getFields()).thenReturn(Set.of());

        assertDoesNotThrow(() -> MappingResolver.validateNoCycles(view));
    }

    // --- Name collision detection ---

    @Test
    void validateNoNameCollisions_givenDuplicateFieldsOnSameParent_throws() {
        var view = mock(LogicalView.class);
        var field1 = mock(Field.class);
        var field2 = mock(Field.class);

        when(view.getResourceName()).thenReturn("testView");
        when(field1.getFieldName()).thenReturn("name");
        lenient().when(field1.getFields()).thenReturn(Set.of());
        when(field2.getFieldName()).thenReturn("name");
        lenient().when(field2.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(field1, field2));
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        assertThat(exception.getMessage(), containsString("name"));
    }

    @Test
    void validateNoNameCollisions_givenJoinFieldCollidesWithBaseField_throws() {
        var view = mock(LogicalView.class);
        var baseField = mock(Field.class);
        var joinField = mock(ExpressionField.class);
        var join = mock(LogicalViewJoin.class);

        when(view.getResourceName()).thenReturn("csvView");
        when(baseField.getFieldName()).thenReturn("name");
        when(baseField.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(baseField));
        when(joinField.getFieldName()).thenReturn("name");
        when(join.getFields()).thenReturn(Set.of(joinField));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        assertThat(exception.getMessage(), containsString("name"));
    }

    @Test
    void validateNoNameCollisions_givenCrossJoinCollision_throws() {
        var view = mock(LogicalView.class);
        var baseField = mock(Field.class);
        var joinField1 = mock(ExpressionField.class);
        var joinField2 = mock(ExpressionField.class);
        var join1 = mock(LogicalViewJoin.class);
        var join2 = mock(LogicalViewJoin.class);

        when(view.getResourceName()).thenReturn("csvView");
        when(baseField.getFieldName()).thenReturn("id");
        when(baseField.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(baseField));
        when(joinField1.getFieldName()).thenReturn("item");
        when(join1.getFields()).thenReturn(Set.of(joinField1));
        when(joinField2.getFieldName()).thenReturn("item");
        when(join2.getFields()).thenReturn(Set.of(joinField2));
        when(view.getLeftJoins()).thenReturn(Set.of(join1, join2));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        assertThat(exception.getMessage(), containsString("item"));
    }

    @Test
    void validateNoNameCollisions_givenNestedDuplicates_throws() {
        var view = mock(LogicalView.class);
        var parent1 = mock(Field.class);
        var parent2 = mock(Field.class);
        var child1 = mock(Field.class);
        var child2 = mock(Field.class);

        when(view.getResourceName()).thenReturn("testView");
        when(parent1.getFieldName()).thenReturn("parent");
        lenient().when(child1.getFieldName()).thenReturn("child");
        lenient().when(child1.getFields()).thenReturn(Set.of());
        lenient().when(parent1.getFields()).thenReturn(Set.of(child1));

        when(parent2.getFieldName()).thenReturn("parent");
        lenient().when(child2.getFieldName()).thenReturn("child");
        lenient().when(child2.getFields()).thenReturn(Set.of());
        lenient().when(parent2.getFields()).thenReturn(Set.of(child2));

        when(view.getFields()).thenReturn(Set.of(parent1, parent2));
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        // Should detect duplicate at "parent" level already
        assertThat(exception.getMessage(), containsString("parent"));
    }

    @Test
    void validateNoNameCollisions_givenDuplicateChildrenUnderSameParent_throws() {
        var view = mock(LogicalView.class);
        var parent = mock(Field.class);
        var child1 = mock(Field.class);
        var child2 = mock(Field.class);

        when(view.getResourceName()).thenReturn("testView");
        when(parent.getFieldName()).thenReturn("address");
        when(child1.getFieldName()).thenReturn("street");
        lenient().when(child1.getFields()).thenReturn(Set.of());
        when(child2.getFieldName()).thenReturn("street");
        lenient().when(child2.getFields()).thenReturn(Set.of());
        when(parent.getFields()).thenReturn(Set.of(child1, child2));
        when(view.getFields()).thenReturn(Set.of(parent));
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        assertThat(exception.getMessage(), containsString("address.street"));
    }

    @Test
    void validateNoNameCollisions_givenInnerJoinFieldCollidesWithBaseField_throws() {
        var view = mock(LogicalView.class);
        var baseField = mock(Field.class);
        var joinField = mock(ExpressionField.class);
        var join = mock(LogicalViewJoin.class);

        when(view.getResourceName()).thenReturn("testView");
        when(baseField.getFieldName()).thenReturn("name");
        when(baseField.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(baseField));
        when(joinField.getFieldName()).thenReturn("name");
        when(join.getFields()).thenReturn(Set.of(joinField));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of(join));

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoNameCollisions(view));

        assertThat(exception.getMessage(), containsString("collision"));
        assertThat(exception.getMessage(), containsString("name"));
    }

    @Test
    void validateNoNameCollisions_givenUniqueFields_doesNotThrow() {
        var view = mock(LogicalView.class);
        var field1 = mock(Field.class);
        var field2 = mock(Field.class);

        when(field1.getFieldName()).thenReturn("name");
        when(field1.getFields()).thenReturn(Set.of());
        when(field2.getFieldName()).thenReturn("age");
        when(field2.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(field1, field2));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of());

        assertDoesNotThrow(() -> MappingResolver.validateNoNameCollisions(view));
    }

    @Test
    void validateNoNameCollisions_givenJoinFieldsUniqueFromBaseFields_doesNotThrow() {
        var view = mock(LogicalView.class);
        var baseField = mock(Field.class);
        var joinField = mock(ExpressionField.class);
        var join = mock(LogicalViewJoin.class);

        when(baseField.getFieldName()).thenReturn("name");
        when(baseField.getFields()).thenReturn(Set.of());
        when(view.getFields()).thenReturn(Set.of(baseField));
        when(joinField.getFieldName()).thenReturn("familyName");
        when(join.getFields()).thenReturn(Set.of(joinField));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        assertDoesNotThrow(() -> MappingResolver.validateNoNameCollisions(view));
    }

    // --- Cycle detection: inner join cycle ---

    @Test
    void validateNoCycles_givenInnerJoinCycle_throwsRuntimeException() {
        var viewA = mock(LogicalView.class);
        var viewB = mock(LogicalView.class);
        var innerJoin = mock(LogicalViewJoin.class);

        when(viewA.getResourceName()).thenReturn("viewA");
        when(viewA.getViewOn()).thenReturn(mock(LogicalSource.class));
        when(viewA.getLeftJoins()).thenReturn(Set.of());
        when(viewA.getInnerJoins()).thenReturn(Set.of(innerJoin));
        lenient().when(viewA.getFields()).thenReturn(Set.of());

        when(innerJoin.getParentLogicalView()).thenReturn(viewB);

        when(viewB.getResourceName()).thenReturn("viewB");
        when(viewB.getViewOn()).thenReturn(viewA);
        lenient().when(viewB.getLeftJoins()).thenReturn(Set.of());
        lenient().when(viewB.getInnerJoins()).thenReturn(Set.of());

        var exception = assertThrows(RmlMapperException.class, () -> MappingResolver.validateNoCycles(viewA));

        assertThat(exception.getMessage(), containsString("Cycle detected in logical view structure"));
    }

    // --- Dedup strategy selection ---

    @Nested
    class DedupStrategySelectionTests {

        @Test
        void selectDedupStrategy_givenNoAnnotations_returnsNone() {
            var view = mock(LogicalView.class);
            when(view.getStructuralAnnotations()).thenReturn(Set.of());

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name"));

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenNullAnnotations_returnsNone() {
            var view = mock(LogicalView.class);
            when(view.getStructuralAnnotations()).thenReturn(null);

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name"));

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenPkCoveringProjectedFields_returnsNone() {
            var view = mock(LogicalView.class);
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("id", "name"));

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenPkNotCoveringProjectedFields_returnsExact() {
            var view = mock(LogicalView.class);
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenEmptyProjectedFieldsWithPk_returnsNone() {
            var view = mock(LogicalView.class);
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk));

            var result = MappingResolver.selectDedupStrategy(view, Set.of());

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenUniquePlusNotNullCoveringProjectedFields_returnsNone() {
            var view = mock(LogicalView.class);
            var unique = mock(UniqueAnnotation.class);
            var notNull = mock(NotNullAnnotation.class);
            var emailField = mock(Field.class);
            when(emailField.getFieldName()).thenReturn("email");
            when(unique.getOnFields()).thenReturn(List.of(emailField));
            when(notNull.getOnFields()).thenReturn(List.of(emailField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(unique, notNull));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("email", "name"));

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenUniqueWithoutNotNull_returnsExact() {
            var view = mock(LogicalView.class);
            var unique = mock(UniqueAnnotation.class);
            var emailField = mock(Field.class);
            when(emailField.getFieldName()).thenReturn("email");
            when(unique.getOnFields()).thenReturn(List.of(emailField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(unique));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("email", "name"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenOnlyIriSafeAnnotation_returnsExact() {
            var view = mock(LogicalView.class);
            var iriSafe = mock(IriSafeAnnotation.class);
            when(view.getStructuralAnnotations()).thenReturn(Set.of(iriSafe));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void resolve_givenExplicitViewWithPkAnnotation_dedupStrategyIsNone() {
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));

            when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(pk));
            when(triplesMap.getLogicalSource()).thenReturn(logicalView);
            when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("id", "name"));

            var result = MappingResolver.resolve(Set.of(triplesMap));

            assertThat(
                    result.get(0).getEvaluationContext().getDedupStrategy(),
                    is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void resolve_givenExplicitViewWithoutCoveringAnnotation_dedupStrategyIsExact() {
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));

            when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(pk));
            when(triplesMap.getLogicalSource()).thenReturn(logicalView);
            when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name"));

            var result = MappingResolver.resolve(Set.of(triplesMap));

            assertThat(
                    result.get(0).getEvaluationContext().getDedupStrategy(),
                    is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenPkWithEmptyOnFields_returnsExact() {
            var view = mock(LogicalView.class);
            var pk = mock(PrimaryKeyAnnotation.class);
            when(pk.getOnFields()).thenReturn(List.of());
            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("id"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenUniqueWithEmptyOnFields_returnsExact() {
            var view = mock(LogicalView.class);
            var unique = mock(UniqueAnnotation.class);
            var notNull = mock(NotNullAnnotation.class);
            when(unique.getOnFields()).thenReturn(List.of());
            when(notNull.getOnFields()).thenReturn(List.of());
            when(view.getStructuralAnnotations()).thenReturn(Set.of(unique, notNull));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("email"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenPkWithNullOnFields_returnsExact() {
            var view = mock(LogicalView.class);
            var pk = mock(PrimaryKeyAnnotation.class);
            when(pk.getOnFields()).thenReturn(null);
            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("id"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenUniqueWithPartialNotNullCoverage_returnsExact() {
            var view = mock(LogicalView.class);
            var unique = mock(UniqueAnnotation.class);
            var notNull = mock(NotNullAnnotation.class);
            var emailField = mock(Field.class);
            var usernameField = mock(Field.class);
            when(emailField.getFieldName()).thenReturn("email");
            when(usernameField.getFieldName()).thenReturn("username");
            when(unique.getOnFields()).thenReturn(List.of(emailField, usernameField));
            when(notNull.getOnFields()).thenReturn(List.of(emailField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(unique, notNull));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("email", "username", "name"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenUniqueNotCoveringProjectedFields_returnsExact() {
            var view = mock(LogicalView.class);
            var unique = mock(UniqueAnnotation.class);
            var notNull = mock(NotNullAnnotation.class);
            var emailField = mock(Field.class);
            when(emailField.getFieldName()).thenReturn("email");
            when(unique.getOnFields()).thenReturn(List.of(emailField));
            when(notNull.getOnFields()).thenReturn(List.of(emailField));
            when(view.getStructuralAnnotations()).thenReturn(Set.of(unique, notNull));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name", "age"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenNotNullOnAllProjected_returnsSimpleEquality() {
            var view = mock(LogicalView.class);
            var notNull = mock(NotNullAnnotation.class);
            var nameField = mock(Field.class);
            var ageField = mock(Field.class);
            when(nameField.getFieldName()).thenReturn("name");
            when(ageField.getFieldName()).thenReturn("age");
            when(notNull.getOnFields()).thenReturn(List.of(nameField, ageField));
            var iriSafe = mock(IriSafeAnnotation.class);
            when(view.getStructuralAnnotations()).thenReturn(Set.of(notNull, iriSafe));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name", "age"));

            assertThat(result, is(instanceOf(DedupStrategy.simpleEquality().getClass())));
        }

        @Test
        void selectDedupStrategy_givenNotNullPartialCoverage_returnsExact() {
            var view = mock(LogicalView.class);
            var notNull = mock(NotNullAnnotation.class);
            var nameField = mock(Field.class);
            when(nameField.getFieldName()).thenReturn("name");
            when(notNull.getOnFields()).thenReturn(List.of(nameField));
            var iriSafe = mock(IriSafeAnnotation.class);
            when(view.getStructuralAnnotations()).thenReturn(Set.of(notNull, iriSafe));

            var result = MappingResolver.selectDedupStrategy(view, Set.of("name", "age"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }

        @Test
        void selectDedupStrategy_givenFkJoinAndPkOnOwnFields_returnsNone() {
            var view = mock(LogicalView.class);
            var parentView = mock(LogicalView.class);

            // PK on own field "id" — but "id" is NOT in projected set
            var pk = mock(PrimaryKeyAnnotation.class);
            var idField = mock(Field.class);
            lenient().when(idField.getFieldName()).thenReturn("id");
            when(pk.getOnFields()).thenReturn(List.of(idField));

            // FK referencing parentView
            var fk = mock(ForeignKeyAnnotation.class);
            when(fk.getTargetView()).thenReturn(parentView);

            when(view.getStructuralAnnotations()).thenReturn(Set.of(pk, fk));

            // Join with parentView contributing "parentName" data field
            var join = mock(LogicalViewJoin.class);
            when(join.getParentLogicalView()).thenReturn(parentView);
            var joinField = mock(ExpressionField.class);
            when(joinField.getFieldName()).thenReturn("parentName");
            when(join.getFields()).thenReturn(Set.of(joinField));

            when(view.getLeftJoins()).thenReturn(Set.of(join));
            lenient().when(view.getInnerJoins()).thenReturn(null);

            // Projected: only "parentName" — PK(id) fails because "id" not projected
            // FK strips "parentName" → effective is empty → none()
            var result = MappingResolver.selectDedupStrategy(view, Set.of("parentName"));

            assertThat(result, is(instanceOf(DedupStrategy.none().getClass())));
        }

        @Test
        void selectDedupStrategy_givenFkJoinButNoOwnFieldCoverage_returnsExact() {
            var view = mock(LogicalView.class);
            var parentView = mock(LogicalView.class);

            // No PK or Unique covering own fields — only FK + IriSafe
            var fk = mock(ForeignKeyAnnotation.class);
            when(fk.getTargetView()).thenReturn(parentView);
            var iriSafe = mock(IriSafeAnnotation.class);
            when(view.getStructuralAnnotations()).thenReturn(Set.of(fk, iriSafe));

            var join = mock(LogicalViewJoin.class);
            when(join.getParentLogicalView()).thenReturn(parentView);
            var joinField = mock(ExpressionField.class);
            when(joinField.getFieldName()).thenReturn("parentName");
            when(join.getFields()).thenReturn(Set.of(joinField));

            when(view.getLeftJoins()).thenReturn(Set.of(join));
            lenient().when(view.getInnerJoins()).thenReturn(null);

            // FK strips "parentName" → effective is {"ownField"}, but no PK/Unique covers it
            var result = MappingResolver.selectDedupStrategy(view, Set.of("ownField", "parentName"));

            assertThat(result, is(instanceOf(DedupStrategy.exact().getClass())));
        }
    }
}
