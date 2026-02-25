package io.carml.engine;

import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.ImplicitViewFactory;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.LogicalView;
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
     * @return a list of ResolvedMapping instances, one per TriplesMap
     */
    public static List<ResolvedMapping> resolve(Set<TriplesMap> triplesMaps) {
        return triplesMaps.stream().map(MappingResolver::resolveOne).toList();
    }

    private static ResolvedMapping resolveOne(TriplesMap triplesMap) {
        var logicalSource = triplesMap.getLogicalSource();

        if (logicalSource instanceof LogicalView logicalView) {
            return resolveExplicit(triplesMap, logicalView);
        }

        return resolveImplicit(triplesMap);
    }

    private static ResolvedMapping resolveExplicit(TriplesMap triplesMap, LogicalView logicalView) {
        var fieldOrigins = buildFieldOrigins(triplesMap, logicalView);
        var evaluationContext = EvaluationContext.withProjectedFields(triplesMap.getReferenceExpressionSet());
        return ResolvedMapping.of(triplesMap, logicalView, false, fieldOrigins, evaluationContext);
    }

    private static ResolvedMapping resolveImplicit(TriplesMap triplesMap) {
        var syntheticView = ImplicitViewFactory.wrap(triplesMap);
        var fieldOrigins = buildFieldOrigins(triplesMap, syntheticView);
        var evaluationContext = EvaluationContext.withProjectedFields(Set.of());
        return ResolvedMapping.of(triplesMap, syntheticView, true, fieldOrigins, evaluationContext);
    }

    /**
     * Builds field origins for all fields in a logical view. Walks the field tree recursively to
     * collect all leaf and intermediate fields.
     */
    private static Map<String, FieldOrigin> buildFieldOrigins(TriplesMap triplesMap, LogicalView view) {
        var origins = new LinkedHashMap<String, FieldOrigin>();
        collectFieldOrigins(triplesMap, view.getFields(), "", origins);
        return origins;
    }

    /**
     * Recursively collects {@link FieldOrigin} entries for all fields. For nested fields, the key
     * uses dot-separated absolute path (e.g. "parent.child"), matching the pattern used by
     * {@code DefaultLogicalViewEvaluator}'s field key collection.
     */
    private static void collectFieldOrigins(
            TriplesMap triplesMap, Set<Field> fields, String prefix, Map<String, FieldOrigin> origins) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            var absoluteName = prefix.isEmpty() ? field.getFieldName() : prefix + "." + field.getFieldName();

            var originalExpression = absoluteName;
            if (field instanceof ExpressionField exprField && exprField.getReference() != null) {
                originalExpression = exprField.getReference();
            }

            origins.put(absoluteName, FieldOrigin.of(originalExpression, triplesMap, field));

            collectFieldOrigins(triplesMap, field.getFields(), absoluteName, origins);
        }
    }
}
