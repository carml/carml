package io.carml.engine;

import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.ImplicitViewFactory;
import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.LogicalView;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TermMap;
import io.carml.model.TriplesMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        var fieldOrigins = buildFieldOrigins(triplesMap, logicalView);
        var evaluationContext =
                EvaluationContext.withProjectedFieldsAndLimit(triplesMap.getReferenceExpressionSet(), limit);
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
}
