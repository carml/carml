package io.carml.logicalview;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.StructuralAnnotation;
import io.carml.model.UniqueAnnotation;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves field dependencies from a {@link LogicalView}'s structural annotations to determine
 * which fields are transitively determined by candidate keys.
 *
 * <p>Dependency extraction rules:
 * <ul>
 *   <li>{@link PrimaryKeyAnnotation} on fields K produces dependency K &rarr; (allViewFields \ K)
 *   <li>{@link UniqueAnnotation} on fields U where all U are covered by {@link NotNullAnnotation}
 *       produces dependency U &rarr; (allViewFields \ U)
 *   <li>{@link ForeignKeyAnnotation} with local fields FK referencing a target view produces
 *       dependency FK &rarr; joinedParentFieldNames, where joined parent field names are collected
 *       from joins whose parent view matches the FK's target view
 * </ul>
 */
@Slf4j
public final class FieldDependencyResolver {

    private FieldDependencyResolver() {}

    /**
     * A functional dependency rule: a set of determinant field names functionally determines a set
     * of dependent field names.
     */
    public record FieldDependency(Set<String> determinant, Set<String> dependent) {}

    /**
     * Extracts field dependencies from a {@link LogicalView}'s structural annotations.
     *
     * @param view the logical view whose annotations define the dependencies
     * @return a set of field dependencies derived from PrimaryKey and Unique+NotNull annotations
     */
    public static Set<FieldDependency> resolveDependencies(LogicalView view) {
        var annotations = view.getStructuralAnnotations();
        if (annotations == null || annotations.isEmpty()) {
            return Set.of();
        }

        var allFieldNames = collectAllFieldNames(view);
        var notNullFieldNames = collectNotNullFieldNames(annotations);
        var dependencies = new LinkedHashSet<FieldDependency>();

        for (var annotation : annotations) {
            if (annotation instanceof PrimaryKeyAnnotation) {
                extractKeyDependency(annotation, allFieldNames).ifPresent(dependencies::add);
            } else if (annotation instanceof UniqueAnnotation) {
                extractUniqueKeyDependency(annotation, allFieldNames, notNullFieldNames)
                        .ifPresent(dependencies::add);
            } else if (annotation instanceof ForeignKeyAnnotation fk) {
                extractForeignKeyDependency(fk, view).ifPresent(dependencies::add);
            }
        }

        LOG.debug("Resolved {} field dependencies from view annotations", dependencies.size());
        return Set.copyOf(dependencies);
    }

    /**
     * Computes the transitive closure of the given field names under the given dependencies using
     * fixed-point iteration.
     *
     * @param fields the initial set of field names
     * @param dependencies the dependency rules to apply transitively
     * @return all field names reachable from {@code fields} via the given dependencies
     */
    public static Set<String> resolveTransitiveCoverage(Set<String> fields, Set<FieldDependency> dependencies) {
        var covered = new LinkedHashSet<>(fields);
        boolean changed = true;
        while (changed) {
            changed = false;
            for (var dependency : dependencies) {
                if (covered.containsAll(dependency.determinant())) {
                    for (var dependent : dependency.dependent()) {
                        if (covered.add(dependent)) {
                            changed = true;
                        }
                    }
                }
            }
        }
        return Set.copyOf(covered);
    }

    /**
     * Checks whether any candidate key (PrimaryKey or Unique+NotNull) transitively determines all
     * the given selected fields.
     *
     * <p>A selected field is considered determined if it appears in the transitive closure of the
     * candidate key's dependencies, <em>excluding</em> the key fields themselves. Key fields are
     * only considered covered when the full key is present in the selected fields (which is handled
     * by the direct PK/Unique coverage check earlier in the dedup pipeline).
     *
     * <p>When {@code selectedFields} is empty, this is interpreted as "all fields are selected",
     * so any candidate key with a non-empty dependent set is sufficient.
     *
     * @param view the logical view whose annotations define candidate keys and dependencies
     * @param selectedFields the field names to check coverage for; empty means all fields
     * @return {@code true} if some candidate key transitively determines all selected fields
     */
    public static boolean isFullyCoveredByKey(LogicalView view, Set<String> selectedFields) {
        var dependencies = resolveDependencies(view);
        if (dependencies.isEmpty()) {
            return false;
        }

        var candidateKeys = collectCandidateKeys(view);
        for (var key : candidateKeys) {
            var coverage = resolveTransitiveCoverage(key, dependencies);
            // The determined fields are those in the transitive closure minus the key itself
            var determinedFields =
                    coverage.stream().filter(f -> !key.contains(f)).collect(toUnmodifiableSet());
            var coversSelected = selectedFields.isEmpty()
                    ? !determinedFields.isEmpty()
                    : determinedFields.containsAll(selectedFields);
            if (coversSelected) {
                LOG.debug(
                        "Candidate key {} transitively determines selected fields {} (determined: {})",
                        key,
                        selectedFields,
                        determinedFields);
                return true;
            }
        }

        return false;
    }

    private static Optional<FieldDependency> extractKeyDependency(
            StructuralAnnotation annotation, Set<String> allFieldNames) {
        var keyFieldNames =
                nullSafeOnFields(annotation).stream().map(Field::getFieldName).collect(toUnmodifiableSet());

        if (keyFieldNames.isEmpty()) {
            return Optional.empty();
        }

        var dependentFields =
                allFieldNames.stream().filter(f -> !keyFieldNames.contains(f)).collect(toUnmodifiableSet());

        return Optional.of(new FieldDependency(keyFieldNames, dependentFields));
    }

    private static Optional<FieldDependency> extractUniqueKeyDependency(
            StructuralAnnotation annotation, Set<String> allFieldNames, Set<String> notNullFieldNames) {
        var uniqueFieldNames =
                nullSafeOnFields(annotation).stream().map(Field::getFieldName).collect(toUnmodifiableSet());

        if (uniqueFieldNames.isEmpty() || !notNullFieldNames.containsAll(uniqueFieldNames)) {
            return Optional.empty();
        }

        var dependentFields = allFieldNames.stream()
                .filter(f -> !uniqueFieldNames.contains(f))
                .collect(toUnmodifiableSet());

        return Optional.of(new FieldDependency(uniqueFieldNames, dependentFields));
    }

    /**
     * Collects all candidate key field name sets from PrimaryKey and Unique+NotNull annotations.
     */
    static Set<Set<String>> collectCandidateKeys(LogicalView view) {
        var annotations = view.getStructuralAnnotations();
        if (annotations == null || annotations.isEmpty()) {
            return Set.of();
        }

        var notNullFieldNames = collectNotNullFieldNames(annotations);
        var keys = new LinkedHashSet<Set<String>>();

        for (var annotation : annotations) {
            if (annotation instanceof PrimaryKeyAnnotation) {
                var keyFields = nullSafeOnFields(annotation).stream()
                        .map(Field::getFieldName)
                        .collect(toUnmodifiableSet());
                if (!keyFields.isEmpty()) {
                    keys.add(keyFields);
                }
            } else if (annotation instanceof UniqueAnnotation) {
                var uniqueFields = nullSafeOnFields(annotation).stream()
                        .map(Field::getFieldName)
                        .collect(toUnmodifiableSet());
                if (!uniqueFields.isEmpty() && notNullFieldNames.containsAll(uniqueFields)) {
                    keys.add(uniqueFields);
                }
            }
        }

        return Set.copyOf(keys);
    }

    private static Optional<FieldDependency> extractForeignKeyDependency(ForeignKeyAnnotation fk, LogicalView view) {
        var fkLocalFields =
                nullSafeOnFields(fk).stream().map(Field::getFieldName).collect(toUnmodifiableSet());
        if (fkLocalFields.isEmpty()) {
            return Optional.empty();
        }

        var targetView = fk.getTargetView();
        if (targetView == null) {
            return Optional.empty();
        }

        var joinedFieldNames = new LinkedHashSet<String>();
        collectJoinedFieldNames(view.getLeftJoins(), targetView, joinedFieldNames);
        collectJoinedFieldNames(view.getInnerJoins(), targetView, joinedFieldNames);

        if (joinedFieldNames.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new FieldDependency(fkLocalFields, Set.copyOf(joinedFieldNames)));
    }

    private static void collectJoinedFieldNames(
            Set<LogicalViewJoin> joins, LogicalView targetView, Set<String> result) {
        if (joins == null) {
            return;
        }
        for (var join : joins) {
            if (join.getParentLogicalView() == targetView) {
                var fields = join.getFields();
                if (fields != null) {
                    fields.stream().map(Field::getFieldName).forEach(result::add);
                }
            }
        }
    }

    private static Set<String> collectNotNullFieldNames(Set<StructuralAnnotation> annotations) {
        return annotations.stream()
                .filter(NotNullAnnotation.class::isInstance)
                .flatMap(a -> nullSafeOnFields(a).stream())
                .map(Field::getFieldName)
                .collect(toUnmodifiableSet());
    }

    /**
     * Collects all field names from a view, including nested fields within {@link IterableField}
     * children.
     */
    private static Set<String> collectAllFieldNames(LogicalView view) {
        var fields = view.getFields();
        if (fields == null || fields.isEmpty()) {
            return Set.of();
        }
        return fields.stream()
                .flatMap(FieldDependencyResolver::flattenFieldNames)
                .collect(toUnmodifiableSet());
    }

    private static Stream<String> flattenFieldNames(Field field) {
        var childFields = field.getFields();
        if (field instanceof IterableField && childFields != null && !childFields.isEmpty()) {
            return Stream.concat(
                    Stream.of(field.getFieldName()),
                    childFields.stream().flatMap(FieldDependencyResolver::flattenFieldNames));
        }
        return Stream.of(field.getFieldName());
    }

    private static List<Field> nullSafeOnFields(StructuralAnnotation annotation) {
        var fields = annotation.getOnFields();
        return fields != null ? fields : List.of();
    }
}
