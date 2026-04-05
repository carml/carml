package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.UniqueAnnotation;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FieldDependencyResolverTest {

    @Test
    void resolveDependencies_givenPk_returnsDependency() {
        var idField = mockField("id");
        var nameField = mockField("name");
        var ageField = mockField("age");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField));

        var view = mockView(Set.of(idField, nameField, ageField), Set.of(pk));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, hasSize(1));
        var dep = dependencies.iterator().next();
        assertThat(dep.determinant(), is(Set.of("id")));
        assertThat(dep.dependent(), is(Set.of("name", "age")));
    }

    @Test
    void resolveDependencies_givenUniqueAndNotNull_returnsDependency() {
        var emailField = mockField("email");
        var nameField = mockField("name");
        var ageField = mockField("age");

        var unique = mock(UniqueAnnotation.class);
        when(unique.getOnFields()).thenReturn(List.of(emailField));

        var notNull = mock(NotNullAnnotation.class);
        when(notNull.getOnFields()).thenReturn(List.of(emailField));

        var view = mockView(Set.of(emailField, nameField, ageField), Set.of(unique, notNull));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, hasSize(1));
        var dep = dependencies.iterator().next();
        assertThat(dep.determinant(), is(Set.of("email")));
        assertThat(dep.dependent(), is(Set.of("name", "age")));
    }

    @Test
    void resolveDependencies_givenNoAnnotations_returnsEmpty() {
        var view = mockView(Set.of(mockField("name")), Set.of());

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, is(empty()));
    }

    @Test
    void resolveDependencies_givenUniqueWithoutNotNull_returnsEmpty() {
        var emailField = mockField("email");
        var nameField = mockField("name");

        var unique = mock(UniqueAnnotation.class);
        when(unique.getOnFields()).thenReturn(List.of(emailField));

        var view = mockView(Set.of(emailField, nameField), Set.of(unique));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, is(empty()));
    }

    @Test
    void resolveTransitiveCoverage_givenChainedDependencies() {
        // A -> B, B -> C => coverage({A}) = {A, B, C}
        var dep1 = new FieldDependencyResolver.FieldDependency(Set.of("A"), Set.of("B"));
        var dep2 = new FieldDependencyResolver.FieldDependency(Set.of("B"), Set.of("C"));

        var coverage = FieldDependencyResolver.resolveTransitiveCoverage(Set.of("A"), Set.of(dep1, dep2));

        assertThat(coverage, is(Set.of("A", "B", "C")));
    }

    @Test
    void resolveTransitiveCoverage_givenMultiStep() {
        // {A, D} -> E, A -> B, B -> C => coverage({A, D}) = {A, B, C, D, E}
        var dep1 = new FieldDependencyResolver.FieldDependency(Set.of("A", "D"), Set.of("E"));
        var dep2 = new FieldDependencyResolver.FieldDependency(Set.of("A"), Set.of("B"));
        var dep3 = new FieldDependencyResolver.FieldDependency(Set.of("B"), Set.of("C"));

        var coverage = FieldDependencyResolver.resolveTransitiveCoverage(Set.of("A", "D"), Set.of(dep1, dep2, dep3));

        assertThat(coverage, is(Set.of("A", "B", "C", "D", "E")));
    }

    @Test
    void isFullyCoveredByKey_givenPkCoverageViaTransitiveDep() {
        // View fields: {id, name, age}
        // PK on {id} => dependency: {id} -> {name, age}
        // Selected: {name, age} — key field "id" not in projection, but PK({id}) determines {name, age}
        var idField = mockField("id");
        var nameField = mockField("name");
        var ageField = mockField("age");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField));

        var view = mockView(Set.of(idField, nameField, ageField), Set.of(pk));

        assertThat(FieldDependencyResolver.isFullyCoveredByKey(view, Set.of("name", "age")), is(true));
    }

    @Test
    void isFullyCoveredByKey_givenCompositeKeyWithPartialKeyInSelected_returnsFalse() {
        // PK on {id, region}, selected {id, name} — region not projected, so PK doesn't
        // guarantee uniqueness. "id" is a key field excluded from determined set.
        var idField = mockField("id");
        var regionField = mockField("region");
        var nameField = mockField("name");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, regionField));

        var view = mockView(Set.of(idField, regionField, nameField), Set.of(pk));

        assertThat(FieldDependencyResolver.isFullyCoveredByKey(view, Set.of("id", "name")), is(false));
    }

    @Test
    void isFullyCoveredByKey_givenNoCandidateKey_returnsFalse() {
        var nameField = mockField("name");
        var ageField = mockField("age");

        var view = mockView(Set.of(nameField, ageField), Set.of());

        assertThat(FieldDependencyResolver.isFullyCoveredByKey(view, Set.of("name", "age")), is(false));
    }

    @Test
    void isFullyCoveredByKey_givenEmptySelectedFields_returnsTrue() {
        var idField = mockField("id");
        var nameField = mockField("name");

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField));

        var view = mockView(Set.of(idField, nameField), Set.of(pk));

        // Empty selectedFields means "all fields" — any candidate key is sufficient
        assertThat(FieldDependencyResolver.isFullyCoveredByKey(view, Set.of()), is(true));
    }

    @Test
    void resolveDependencies_givenForeignKey_returnsDependency() {
        var studentIdField = mockField("student_id");
        var nameField = mockField("name");

        var parentView = mock(LogicalView.class);

        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(studentIdField, nameField), Set.of(fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, hasSize(1));
        var dep = dependencies.iterator().next();
        assertThat(dep.determinant(), is(Set.of("student_id")));
        assertThat(dep.dependent(), is(Set.of("_join0.student_name")));
    }

    @Test
    void resolveDependencies_givenForeignKeyWithNoMatchingJoin_returnsEmpty() {
        var studentIdField = mockField("student_id");

        var parentView = mock(LogicalView.class);
        var otherView = mock(LogicalView.class);

        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(otherView);
        lenient().when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(studentIdField), Set.of(fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, is(empty()));
    }

    @Test
    void resolveDependencies_givenForeignKeyWithNullTargetView_returnsEmpty() {
        var studentIdField = mockField("student_id");

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(null);

        var view = mockView(Set.of(studentIdField), Set.of(fk));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, is(empty()));
    }

    @Test
    void resolveDependencies_givenForeignKeyWithEmptyOnFields_returnsEmpty() {
        var parentView = mock(LogicalView.class);

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of());
        lenient().when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(mockField("a")), Set.of(fk));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, is(empty()));
    }

    @Test
    void resolveDependencies_givenPkAndForeignKey_returnsBothDependencies() {
        var idField = mockField("id");
        var studentIdField = mockField("student_id");
        var nameField = mockField("name");

        var parentView = mock(LogicalView.class);

        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(List.of(idField, studentIdField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(idField, studentIdField, nameField), Set.of(pk, fk));
        when(view.getLeftJoins()).thenReturn(Set.of(join));
        when(view.getInnerJoins()).thenReturn(Set.of());

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, hasSize(2));
    }

    @Test
    void resolveDependencies_givenForeignKeyWithInnerJoin_returnsDependency() {
        var studentIdField = mockField("student_id");

        var parentView = mock(LogicalView.class);

        var joinField = mockExpressionField("_join0.student_name");
        var join = mock(LogicalViewJoin.class);
        when(join.getParentLogicalView()).thenReturn(parentView);
        when(join.getFields()).thenReturn(Set.of(joinField));

        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getOnFields()).thenReturn(List.of(studentIdField));
        when(fk.getTargetView()).thenReturn(parentView);

        var view = mockView(Set.of(studentIdField), Set.of(fk));
        when(view.getLeftJoins()).thenReturn(Set.of());
        when(view.getInnerJoins()).thenReturn(Set.of(join));

        var dependencies = FieldDependencyResolver.resolveDependencies(view);

        assertThat(dependencies, hasSize(1));
        var dep = dependencies.iterator().next();
        assertThat(dep.determinant(), is(Set.of("student_id")));
        assertThat(dep.dependent(), is(Set.of("_join0.student_name")));
    }

    private static ExpressionField mockExpressionField(String fieldName) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        return field;
    }

    private static Field mockField(String fieldName) {
        var field = mock(Field.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        return field;
    }

    private static LogicalView mockView(
            Set<Field> fields, Set<? extends io.carml.model.StructuralAnnotation> annotations) {
        var view = mock(LogicalView.class);
        lenient().when(view.getFields()).thenReturn(fields);
        @SuppressWarnings("unchecked")
        var typedAnnotations = (Set<io.carml.model.StructuralAnnotation>) (Set<?>) annotations;
        lenient().when(view.getStructuralAnnotations()).thenReturn(typedAnnotations);
        return view;
    }
}
