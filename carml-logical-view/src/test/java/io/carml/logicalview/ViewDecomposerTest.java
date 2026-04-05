package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.Join;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PredicateObjectMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.RefObjectMap;
import io.carml.model.StructuralAnnotation;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.UniqueAnnotation;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ViewDecomposerTest {

    @Test
    void decompose_givenNoAnnotations_returnsSingleGroupNotDecomposed() {
        var view = mockView(Set.of(mockField("id"), mockField("name")), Set.of());

        var pom = mockPom(Set.of("name"), false);
        var triplesMap = mockTriplesMap(Set.of(pom), Set.of("id"), Set.of("id", "name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        assertThat(result.isDecomposed(), is(false));
        assertThat(result.groups(), hasSize(1));
        assertThat(result.groups().get(0).predicateObjectMaps(), is(Set.of(pom)));
        assertThat(result.groups().get(0).emitsClassTriples(), is(true));
    }

    @Test
    void decompose_givenPkOnlyAllPomsFullKey_returnsSingleGroupNotDecomposed() {
        var idField = mockField("id");
        var nameField = mockField("name");
        var ageField = mockField("age");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField));

        var view = mockView(Set.of(idField, nameField, ageField), Set.of(pk));

        // Both POMs reference fields determined by the full key (id -> name, age)
        var pom1 = mockPom(Set.of("name"), false);
        var pom2 = mockPom(Set.of("age"), false);

        var triplesMap = mockTriplesMap(Set.of(pom1, pom2), Set.of("id"), Set.of("id", "name", "age"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        // Both POMs have the same determinant {id}, so only one group → not decomposed
        assertThat(result.isDecomposed(), is(false));
        assertThat(result.groups(), hasSize(1));
    }

    @Test
    void decompose_givenPkAndFk_returnsTwoGroups() {
        // PK on {id, student_id}, FK on {student_id} -> parent view
        // Join from parent view gives us _join0.student_name
        // POM1 uses "name" (determined by full PK)
        // POM2 uses "_join0.student_name" (determined by FK subset {student_id})
        var idField = mockField("id");
        var studentIdField = mockField("student_id");
        var nameField = mockField("name");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var parentView = mock(LogicalView.class);
        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField, nameField), Set.of(pk, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var pom1 = mockPom(Set.of("name"), false);
        var pom2 = mockPom(Set.of("_join0.student_name"), false);

        // Subject uses "id" and "student_id"
        var triplesMap = mockTriplesMap(
                Set.of(pom1, pom2),
                Set.of("id", "student_id"),
                Set.of("id", "student_id", "name", "_join0.student_name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        assertThat(result.isDecomposed(), is(true));
        assertThat(result.groups(), hasSize(2));

        // Find the group containing pom2 (join field group, determinant = {student_id})
        var joinGroup = result.groups().stream()
                .filter(g -> g.predicateObjectMaps().contains(pom2))
                .findFirst()
                .orElseThrow();
        assertThat(joinGroup.projectedFields().contains("_join0.student_name"), is(true));
        assertThat(joinGroup.projectedFields().contains("student_id"), is(true));

        // The full-key group (pom1 referencing "name")
        var fullKeyGroup = result.groups().stream()
                .filter(g -> g.predicateObjectMaps().contains(pom1))
                .findFirst()
                .orElseThrow();
        assertThat(fullKeyGroup.projectedFields().contains("name"), is(true));

        // Exactly one group emits class triples
        var classTripleCount = result.groups().stream()
                .filter(ViewDecomposer.DecompositionGroup::emitsClassTriples)
                .count();
        assertThat(classTripleCount, is(1L));
    }

    @Test
    void decompose_givenPomWithRefObjectMapJoin_goesToFullKeyGroup() {
        var idField = mockField("id");
        var studentIdField = mockField("student_id");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var parentView = mock(LogicalView.class);
        var joinField = mockExpressionField("_join0.student_name");
        var lvJoin = mock(LogicalViewJoin.class);
        when(lvJoin.getParentLogicalView()).thenReturn(parentView);
        when(lvJoin.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField), Set.of(pk, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(lvJoin));
        when(view.getInnerJoins()).thenReturn(Set.of());

        // POM with join condition (RefObjectMap) - should go to full-key group
        var pomWithJoin = mockPom(Set.of(), true);

        // POM without join, uses field determined by FK subset
        var pomSimple = mockPom(Set.of("_join0.student_name"), false);

        var triplesMap = mockTriplesMap(
                Set.of(pomWithJoin, pomSimple),
                Set.of("id", "student_id"),
                Set.of("id", "student_id", "_join0.student_name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        assertThat(result.isDecomposed(), is(true));
        assertThat(result.groups(), hasSize(2));

        // pomWithJoin should be in the full-key group
        var fullKeyGroup = result.groups().stream()
                .filter(g -> g.predicateObjectMaps().contains(pomWithJoin))
                .findFirst()
                .orElseThrow();
        assertThat(fullKeyGroup.predicateObjectMaps(), is(Set.of(pomWithJoin)));
    }

    @Test
    void decompose_classTriples_assignedToNarrowestDeterminantGroup() {
        var idField = mockField("id");
        var studentIdField = mockField("student_id");
        var nameField = mockField("name");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var parentView = mock(LogicalView.class);
        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField, nameField), Set.of(pk, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var pom1 = mockPom(Set.of("name"), false);
        var pom2 = mockPom(Set.of("_join0.student_name"), false);

        var triplesMap = mockTriplesMap(
                Set.of(pom1, pom2),
                Set.of("id", "student_id"),
                Set.of("id", "student_id", "name", "_join0.student_name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        assertThat(result.isDecomposed(), is(true));

        // Exactly one group should emit class triples
        var classTripleGroups = result.groups().stream()
                .filter(ViewDecomposer.DecompositionGroup::emitsClassTriples)
                .toList();
        assertThat(classTripleGroups, hasSize(1));
    }

    @Test
    void decompose_allPomsSameDeterminant_returnsSingleGroupNotDecomposed() {
        var idField = mockField("id");
        var studentIdField = mockField("student_id");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var parentView = mock(LogicalView.class);
        var joinField1 = mockExpressionField("_join0.name");
        var joinField2 = mockExpressionField("_join0.email");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField1, joinField2));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField), Set.of(pk, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        // Both POMs use fields determined by same FK subset {student_id}
        var pom1 = mockPom(Set.of("_join0.name"), false);
        var pom2 = mockPom(Set.of("_join0.email"), false);

        var triplesMap = mockTriplesMap(
                Set.of(pom1, pom2),
                Set.of("id", "student_id"),
                Set.of("id", "student_id", "_join0.name", "_join0.email"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        // All POMs have same determinant → single group, not decomposed
        assertThat(result.isDecomposed(), is(false));
        assertThat(result.groups(), hasSize(1));
    }

    @Test
    void decompose_givenNoCandidateKeys_returnsSingleGroupNotDecomposed() {
        // Only FK annotation, no PK or Unique — so no candidate keys
        var studentIdField = mockField("student_id");

        var parentView = mock(LogicalView.class);
        var joinField = mockExpressionField("_join0.name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(studentIdField), Set.of(fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var pom = mockPom(Set.of("_join0.name"), false);
        var triplesMap = mockTriplesMap(Set.of(pom), Set.of("student_id"), Set.of("student_id", "_join0.name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        assertThat(result.isDecomposed(), is(false));
        assertThat(result.groups(), hasSize(1));
    }

    @Test
    void decompose_givenTwoCandidateKeys_usesMinimalDeterminant() {
        // With only PK on {id, student_id} and FK on {student_id}, decomposition produces
        // 2 groups: full-PK group and FK-subset group (see decompose_givenPkAndFk_returnsTwoGroups).
        //
        // Adding Unique+NotNull on {student_id} introduces a second candidate key. Now
        // {student_id} alone transitively determines all view fields (Unique dep:
        // {student_id} -> {id, name}). This means every POM's fields are determined by
        // {student_id}, so all POMs share the same minimal determinant and decomposition
        // collapses into a single group.
        var idField = mockField("id");
        var studentIdField = mockField("student_id");
        var nameField = mockField("name");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var unique = mock(UniqueAnnotation.class);
        when(unique.getOnFields()).thenReturn(List.of(studentIdField));

        var notNull = mock(NotNullAnnotation.class);
        when(notNull.getOnFields()).thenReturn(List.of(studentIdField));

        var parentView = mock(LogicalView.class);
        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField, nameField), Set.of(pk, unique, notNull, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var pom1 = mockPom(Set.of("name"), false);
        var pom2 = mockPom(Set.of("_join0.student_name"), false);

        var triplesMap = mockTriplesMap(
                Set.of(pom1, pom2),
                Set.of("id", "student_id"),
                Set.of("id", "student_id", "name", "_join0.student_name"));

        var result = ViewDecomposer.decompose(triplesMap, view);

        // Unique+NotNull on {student_id} makes it a candidate key that determines all fields,
        // so both POMs share the same determinant -> single group, not decomposed
        assertThat(result.isDecomposed(), is(false));
        assertThat(result.groups(), hasSize(1));
        assertThat(result.groups().get(0).predicateObjectMaps(), is(Set.of(pom1, pom2)));
        assertThat(result.groups().get(0).emitsClassTriples(), is(true));
    }

    private static Field mockField(String fieldName) {
        var field = mock(Field.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        return field;
    }

    private static ExpressionField mockExpressionField(String fieldName) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        return field;
    }

    private static LogicalView mockView(Set<Field> fields, Set<? extends StructuralAnnotation> annotations) {
        var view = mock(LogicalView.class);
        lenient().when(view.getFields()).thenReturn(fields);
        @SuppressWarnings("unchecked")
        var typedAnnotations = (Set<StructuralAnnotation>) (Set<?>) annotations;
        lenient().when(view.getStructuralAnnotations()).thenReturn(typedAnnotations);
        return view;
    }

    private static PredicateObjectMap mockPom(Set<String> referenceExpressions, boolean hasJoinCondition) {
        var pom = mock(PredicateObjectMap.class);
        lenient().when(pom.getReferenceExpressionSet()).thenReturn(referenceExpressions);
        lenient().when(pom.getPredicateMaps()).thenReturn(Set.of());
        lenient().when(pom.getGraphMaps()).thenReturn(Set.of());

        if (hasJoinCondition) {
            var joinCondition = mock(Join.class);
            var refObjectMap = mock(RefObjectMap.class);
            lenient().when(refObjectMap.getJoinConditions()).thenReturn(Set.of(joinCondition));
            lenient().when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap));
        } else {
            lenient().when(pom.getObjectMaps()).thenReturn(Set.of());
        }

        return pom;
    }

    private static TriplesMap mockTriplesMap(
            Set<PredicateObjectMap> poms, Set<String> subjectExpressions, Set<String> allExpressions) {
        var triplesMap = mock(TriplesMap.class);
        lenient().when(triplesMap.getPredicateObjectMaps()).thenReturn(poms);
        lenient().when(triplesMap.getReferenceExpressionSet()).thenReturn(allExpressions);

        var subjectMap = mock(SubjectMap.class);
        lenient().when(subjectMap.getReferenceExpressionSet()).thenReturn(subjectExpressions);
        lenient().when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));

        // Set up per-POM reference expression set
        for (var pom : poms) {
            var pomExprs = pom.getReferenceExpressionSet();
            var subjectAndPomExprs = new java.util.LinkedHashSet<>(subjectExpressions);
            subjectAndPomExprs.addAll(pomExprs);
            lenient()
                    .when(triplesMap.getReferenceExpressionSet(Set.of(pom)))
                    .thenReturn(Set.copyOf(subjectAndPomExprs));
        }

        // Also set up for any combination of POMs (for the single-group case)
        lenient().when(triplesMap.getReferenceExpressionSet(poms)).thenReturn(allExpressions);

        return triplesMap;
    }
}
