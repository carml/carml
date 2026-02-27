package io.carml.engine;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.logicalview.DedupStrategy;
import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.ImplicitViewFactory;
import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.RefObjectMap;
import io.carml.model.StructuralAnnotation;
import io.carml.model.TermMap;
import io.carml.model.TriplesMap;
import io.carml.model.UniqueAnnotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Resolves a set of {@link TriplesMap} instances into {@link ResolvedMapping} instances.
 *
 * <p>For each TriplesMap, determines whether it uses an explicit {@link LogicalView} or a bare
 * {@code LogicalSource}, and produces the corresponding {@link ResolvedMapping}. Bare LogicalSource
 * mappings are wrapped in a synthetic LogicalView via {@link ImplicitViewFactory}.
 */
public final class MappingResolver {

    private MappingResolver() {}

    /**
     * Resolves a set of TriplesMaps into ResolvedMapping instances with no iteration limit.
     *
     * <p>Equivalent to calling {@link #resolve(Set, Long) resolve(triplesMaps, null)}.
     *
     * @param triplesMaps the set of TriplesMaps to resolve
     * @return a list of ResolvedMapping instances, one per TriplesMap
     */
    public static List<ResolvedMapping> resolve(Set<TriplesMap> triplesMaps) {
        return resolve(triplesMaps, null);
    }

    /**
     * Resolves a set of TriplesMaps into ResolvedMapping instances.
     *
     * <p>For each TriplesMap:
     * <ul>
     *   <li>If its logicalSource is a {@link LogicalView}, the view is used directly (explicit view)
     *   <li>Otherwise, a synthetic LogicalView is created via {@link ImplicitViewFactory#wrap(TriplesMap)}
     *       (implicit view)
     * </ul>
     *
     * @param triplesMaps the set of TriplesMaps to resolve
     * @param limit the maximum number of iterations to produce per mapping, or {@code null} for no limit
     * @return a list of ResolvedMapping instances, one per TriplesMap
     */
    public static List<ResolvedMapping> resolve(Set<TriplesMap> triplesMaps, Long limit) {
        return triplesMaps.stream().map(tm -> resolveOne(tm, limit)).toList();
    }

    private static ResolvedMapping resolveOne(TriplesMap triplesMap, Long limit) {
        var logicalSource = triplesMap.getLogicalSource();

        if (logicalSource instanceof LogicalView logicalView) {
            return resolveExplicit(triplesMap, logicalView, limit);
        }

        return resolveImplicit(triplesMap, limit);
    }

    private static ResolvedMapping resolveExplicit(TriplesMap triplesMap, LogicalView logicalView, Long limit) {
        validateNoCycles(logicalView);
        validateNoNameCollisions(logicalView);
        var fieldOrigins = buildFieldOrigins(triplesMap, logicalView);
        var projectedFields = triplesMap.getReferenceExpressionSet();
        var dedupStrategy = selectDedupStrategy(logicalView, projectedFields);
        var evaluationContext = EvaluationContext.of(projectedFields, dedupStrategy, limit);
        return ResolvedMapping.of(triplesMap, logicalView, false, fieldOrigins, evaluationContext);
    }

    private static ResolvedMapping resolveImplicit(TriplesMap triplesMap, Long limit) {
        var syntheticView = ImplicitViewFactory.wrap(triplesMap);
        var expressionToTermMap = buildExpressionToTermMap(triplesMap);
        var fieldOrigins = buildFieldOrigins(triplesMap, syntheticView, expressionToTermMap);
        var evaluationContext = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), limit);
        return ResolvedMapping.of(triplesMap, syntheticView, true, fieldOrigins, evaluationContext);
    }

    /**
     * Selects the appropriate {@link DedupStrategy} based on the structural annotations of a
     * {@link LogicalView} and the set of projected fields.
     *
     * <p>Returns {@link DedupStrategy#none()} (no dedup needed) when:
     * <ul>
     *   <li>No structural annotations exist</li>
     *   <li>A {@link PrimaryKeyAnnotation} covers the projected fields</li>
     *   <li>A {@link UniqueAnnotation} covers the projected fields and all its fields are also
     *       covered by a {@link NotNullAnnotation}</li>
     * </ul>
     *
     * <p>Otherwise returns {@link DedupStrategy#exact()}.
     */
    static DedupStrategy selectDedupStrategy(LogicalView logicalView, Set<String> projectedFields) {
        var annotations = logicalView.getStructuralAnnotations();
        if (annotations == null || annotations.isEmpty()) {
            return DedupStrategy.none();
        }

        var notNullFields = annotations.stream()
                .filter(NotNullAnnotation.class::isInstance)
                .flatMap(a -> nullSafeOnFields(a).stream())
                .map(Field::getFieldName)
                .collect(toUnmodifiableSet());

        if (hasCoveringPkOrUnique(annotations, notNullFields, projectedFields)) {
            return DedupStrategy.none();
        }

        // FK on joins → strip FK-guaranteed join fields from effective projection, re-check
        var fkJoinFieldNames = collectFkGuaranteedJoinFieldNames(logicalView);
        if (!fkJoinFieldNames.isEmpty()) {
            var effectiveProjected = projectedFields.stream()
                    .filter(f -> !fkJoinFieldNames.contains(f))
                    .collect(toUnmodifiableSet());
            if (hasCoveringPkOrUnique(annotations, notNullFields, effectiveProjected)) {
                return DedupStrategy.none();
            }
        }

        // NotNull on all projected fields → simpleEquality()
        if (!projectedFields.isEmpty() && notNullFields.containsAll(projectedFields)) {
            return DedupStrategy.simpleEquality();
        }

        return DedupStrategy.exact();
    }

    private static boolean hasCoveringPkOrUnique(
            Set<StructuralAnnotation> annotations, Set<String> notNullFields, Set<String> projectedFields) {
        var hasPk = annotations.stream()
                .filter(PrimaryKeyAnnotation.class::isInstance)
                .anyMatch(a -> {
                    var pkFields = nullSafeOnFields(a).stream()
                            .map(Field::getFieldName)
                            .collect(toUnmodifiableSet());
                    return !pkFields.isEmpty() && projectionCovers(projectedFields, pkFields);
                });

        if (hasPk) {
            return true;
        }

        return annotations.stream().filter(UniqueAnnotation.class::isInstance).anyMatch(a -> {
            var uniqueFields =
                    nullSafeOnFields(a).stream().map(Field::getFieldName).collect(toUnmodifiableSet());
            return !uniqueFields.isEmpty()
                    && projectionCovers(projectedFields, uniqueFields)
                    && notNullFields.containsAll(uniqueFields);
        });
    }

    private static Set<String> collectFkGuaranteedJoinFieldNames(LogicalView logicalView) {
        var fkFieldNames = new HashSet<String>();
        var annotations = logicalView.getStructuralAnnotations();
        if (annotations == null) {
            return Set.of();
        }
        collectFkJoinFieldNamesFromJoins(logicalView.getLeftJoins(), annotations, fkFieldNames);
        collectFkJoinFieldNamesFromJoins(logicalView.getInnerJoins(), annotations, fkFieldNames);
        return fkFieldNames;
    }

    private static void collectFkJoinFieldNamesFromJoins(
            Set<LogicalViewJoin> joins, Set<StructuralAnnotation> annotations, Set<String> fkFieldNames) {
        if (joins == null) {
            return;
        }
        for (var join : joins) {
            var hasFk = annotations.stream()
                    .anyMatch(a ->
                            a instanceof ForeignKeyAnnotation fk && fk.getTargetView() == join.getParentLogicalView());
            if (hasFk && join.getFields() != null) {
                join.getFields().stream().map(ExpressionField::getFieldName).forEach(fkFieldNames::add);
            }
        }
    }

    /** Empty projectedFields means "all fields", so any required set is covered. */
    private static boolean projectionCovers(Set<String> projectedFields, Set<String> required) {
        return projectedFields.isEmpty() || projectedFields.containsAll(required);
    }

    private static List<Field> nullSafeOnFields(StructuralAnnotation annotation) {
        var fields = annotation.getOnFields();
        return fields != null ? fields : List.of();
    }

    /**
     * Builds field origins for all fields in a logical view. Walks the field tree recursively to
     * collect all leaf and intermediate fields.
     */
    private static Map<String, FieldOrigin> buildFieldOrigins(TriplesMap triplesMap, LogicalView view) {
        return buildFieldOrigins(triplesMap, view, Map.of());
    }

    private static Map<String, FieldOrigin> buildFieldOrigins(
            TriplesMap triplesMap, LogicalView view, Map<String, TermMap> expressionToTermMap) {
        var origins = new LinkedHashMap<String, FieldOrigin>();
        collectFieldOrigins(triplesMap, view.getFields(), "", origins, expressionToTermMap);
        return origins;
    }

    /**
     * Recursively collects {@link FieldOrigin} entries for all fields. For nested fields, the key
     * uses dot-separated absolute path (e.g. "parent.child"), matching the pattern used by
     * {@code DefaultLogicalViewEvaluator}'s field key collection.
     */
    private static void collectFieldOrigins(
            TriplesMap triplesMap,
            Set<Field> fields,
            String prefix,
            Map<String, FieldOrigin> origins,
            Map<String, TermMap> expressionToTermMap) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            var absoluteName = prefix.isEmpty() ? field.getFieldName() : prefix + "." + field.getFieldName();

            var originalExpression = absoluteName;
            if (field instanceof ExpressionField exprField && exprField.getReference() != null) {
                originalExpression = exprField.getReference();
            }

            var termMap = expressionToTermMap.get(originalExpression);
            if (termMap != null) {
                origins.put(absoluteName, FieldOrigin.of(originalExpression, termMap, triplesMap, field));
            } else {
                origins.put(absoluteName, FieldOrigin.of(originalExpression, triplesMap, field));
            }

            collectFieldOrigins(triplesMap, field.getFields(), absoluteName, origins, expressionToTermMap);
        }
    }

    /**
     * Builds a mapping from reference expressions to the TermMap that contributed them. Walks all
     * SubjectMaps, PredicateMaps, ObjectMaps, GraphMaps, and join ChildMaps. When multiple TermMaps
     * share an expression, the first one encountered wins.
     */
    private static Map<String, TermMap> buildExpressionToTermMap(TriplesMap triplesMap) {
        var result = new LinkedHashMap<String, TermMap>();

        for (var subjectMap : triplesMap.getSubjectMaps()) {
            collectExpressionMapTermMap(subjectMap, result);
            for (var graphMap : subjectMap.getGraphMaps()) {
                collectExpressionMapTermMap(graphMap, result);
            }
        }

        for (var pom : triplesMap.getPredicateObjectMaps()) {
            collectPredicateObjectMapTermMaps(pom, result);
        }

        return result;
    }

    private static void collectPredicateObjectMapTermMaps(PredicateObjectMap pom, Map<String, TermMap> result) {
        for (var predicateMap : pom.getPredicateMaps()) {
            collectExpressionMapTermMap(predicateMap, result);
        }
        for (var objectMap : pom.getObjectMaps()) {
            if (objectMap instanceof ObjectMap om) {
                collectExpressionMapTermMap(om, result);
            } else if (objectMap instanceof RefObjectMap rom) {
                for (var join : rom.getJoinConditions()) {
                    collectExpressionMapTermMap(join.getChildMap(), result);
                }
            }
        }
        for (var graphMap : pom.getGraphMaps()) {
            collectExpressionMapTermMap(graphMap, result);
        }
    }

    private static void collectExpressionMapTermMap(ExpressionMap expressionMap, Map<String, TermMap> result) {
        var termMap = expressionMap instanceof TermMap tm ? tm : null;
        for (var expression : expressionMap.getExpressionMapExpressionSet()) {
            if (termMap != null) {
                result.putIfAbsent(expression, termMap);
            }
        }
    }

    /**
     * Validates that a {@link LogicalView} contains no circular references. Checks both the view
     * structure (viewOn and join edges) and the field tree for cycles.
     *
     * @param view the logical view to validate
     * @throws RmlMapperException if a cycle is detected
     */
    static void validateNoCycles(LogicalView view) {
        validateViewNoCycles(view, new IdentityHashMap<>(), new ArrayList<>());
        validateFieldNoCycles(view.getFields(), new IdentityHashMap<>(), new ArrayList<>());
    }

    /**
     * Performs a depth-first search through viewOn and join parentLogicalView edges to detect cycles
     * in the logical view graph. Uses identity-based comparison since view objects may not have
     * meaningful equals/hashCode for cycle detection purposes.
     */
    private static void validateViewNoCycles(
            LogicalView view, IdentityHashMap<LogicalView, Boolean> visited, List<String> path) {
        var viewName = view.getResourceName();

        if (visited.containsKey(view)) {
            path.add(viewName);
            throw new RmlMapperException(
                    "Cycle detected in logical view structure: %s".formatted(String.join(" -> ", path)));
        }

        visited.put(view, Boolean.TRUE);
        path.add(viewName);

        // Follow the viewOn edge if it leads to another LogicalView
        var viewOn = view.getViewOn();
        if (viewOn instanceof LogicalView parentView) {
            validateViewNoCycles(parentView, visited, path);
        }

        // Follow join parentLogicalView edges (both left joins and inner joins)
        var allJoins = Stream.concat(nullSafe(view.getLeftJoins()).stream(), nullSafe(view.getInnerJoins()).stream())
                .toList();

        for (var join : allJoins) {
            var parentView = join.getParentLogicalView();
            if (parentView != null) {
                validateViewNoCycles(parentView, visited, path);
            }
        }

        path.remove(path.size() - 1);
        visited.remove(view);
    }

    /**
     * Performs a depth-first search through child field edges to detect cycles in the field tree.
     * Uses identity-based comparison since field objects may not have meaningful equals/hashCode.
     */
    private static void validateFieldNoCycles(
            Set<Field> fields, IdentityHashMap<Field, Boolean> visited, List<String> path) {
        if (fields == null) {
            return;
        }

        for (var field : fields) {
            var fieldName = field.getFieldName();

            if (visited.containsKey(field)) {
                path.add(fieldName);
                throw new RmlMapperException("Cycle detected in field tree: %s".formatted(String.join(" -> ", path)));
            }

            visited.put(field, Boolean.TRUE);
            path.add(fieldName);
            validateFieldNoCycles(field.getFields(), visited, path);
            path.remove(path.size() - 1);
            visited.remove(field);
        }
    }

    /**
     * Validates that a {@link LogicalView} contains no duplicate absolute field names. Checks the
     * view's own field tree (recursively, using dot-separated paths) and all join fields (left joins
     * and inner joins). Duplicate names across any of these sources are rejected.
     *
     * @param view the logical view to validate
     * @throws RmlMapperException if a duplicate field name is detected
     */
    static void validateNoNameCollisions(LogicalView view) {
        var seen = new HashSet<String>();
        collectFieldNamesForCollisionCheck(view.getFields(), "", seen, view);

        var allJoins = Stream.concat(nullSafe(view.getLeftJoins()).stream(), nullSafe(view.getInnerJoins()).stream())
                .toList();

        for (var join : allJoins) {
            for (var field : nullSafe(join.getFields())) {
                var fieldName = field.getFieldName();
                if (!seen.add(fieldName)) {
                    throw new RmlMapperException("Name collision detected in logical view %s: duplicate field name '%s'"
                            .formatted(view.getResourceName(), fieldName));
                }
            }
        }
    }

    /**
     * Recursively collects absolute field names from the view's field tree for collision detection.
     * Uses dot-separated paths for nested fields (e.g. "parent.child").
     */
    private static void collectFieldNamesForCollisionCheck(
            Set<Field> fields, String prefix, Set<String> seen, LogicalView view) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            var absoluteName = prefix.isEmpty() ? field.getFieldName() : prefix + "." + field.getFieldName();
            if (!seen.add(absoluteName)) {
                throw new RmlMapperException("Name collision detected in logical view %s: duplicate field name '%s'"
                        .formatted(view.getResourceName(), absoluteName));
            }
            collectFieldNamesForCollisionCheck(field.getFields(), absoluteName, seen, view);
        }
    }

    private static <T> Set<T> nullSafe(Set<T> set) {
        return set != null ? set : Set.of();
    }
}
