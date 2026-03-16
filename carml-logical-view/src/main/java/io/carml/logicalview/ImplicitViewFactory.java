package io.carml.logicalview;

import io.carml.model.BaseObjectMap;
import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.ObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlExpressionField;
import io.carml.model.impl.CarmlLogicalView;
import io.carml.model.impl.CarmlLogicalViewJoin;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates synthetic {@link LogicalView} instances that wrap bare {@link LogicalSource} mappings.
 * Each referenced expression in the TriplesMap becomes an auto-derived field. RefObjectMaps with
 * join conditions are converted to left joins on the synthetic view.
 */
public class ImplicitViewFactory {

    private ImplicitViewFactory() {}

    /**
     * Result of wrapping a TriplesMap in a synthetic LogicalView.
     *
     * @param view the synthetic LogicalView
     * @param refObjectMapPrefixes mapping from each handled RefObjectMap to its expression prefix
     *     (e.g. {@code "_ref0."})
     */
    public record WrapResult(
            LogicalView view, Map<RefObjectMap, String> refObjectMapPrefixes, Set<LogicalViewJoin> aggregatingJoins) {}

    /**
     * Wraps a TriplesMap's bare LogicalSource in a synthetic LogicalView. RefObjectMaps with
     * non-empty join conditions (that are not self-joining) are converted to left joins on the view.
     *
     * @param triplesMap the TriplesMap to wrap
     * @return a {@link WrapResult} containing the synthetic view and RefObjectMap prefix mappings
     */
    public static WrapResult wrap(TriplesMap triplesMap) {
        Objects.requireNonNull(triplesMap, "triplesMap must not be null");
        var logicalSource = triplesMap.getLogicalSource();
        Objects.requireNonNull(logicalSource, "triplesMap must have a logicalSource");

        var fields = collectViewFields(triplesMap);

        // Only process left joins for LogicalSource-based TriplesMaps
        if (!(logicalSource instanceof LogicalSource)) {
            var view = CarmlLogicalView.builder()
                    .viewOn(logicalSource)
                    .fields(fields)
                    .build();

            return new WrapResult(view, Map.of(), Set.of());
        }

        // Collect RefObjectMaps with non-empty join conditions that are not self-joining
        var joiningRefObjectMaps = collectJoiningRefObjectMaps(triplesMap);

        // Collect RefObjectMaps from rml:gather with non-empty join conditions.
        // These need aggregating joins that collect all matching parent values into lists.
        var gatherJoiningRefObjectMaps = collectGatherJoiningRefObjectMaps(triplesMap);
        var aggregatingRoms = new LinkedHashSet<>(gatherJoiningRefObjectMaps);

        // Combine both lists, deduplicating
        var allJoiningRoms = new LinkedHashSet<>(joiningRefObjectMaps);
        allJoiningRoms.addAll(gatherJoiningRefObjectMaps);

        if (allJoiningRoms.isEmpty()) {
            var view = CarmlLogicalView.builder()
                    .viewOn(logicalSource)
                    .fields(fields)
                    .build();

            return new WrapResult(view, Map.of(), Set.of());
        }

        // Sort deterministically for stable prefix assignment
        var sortedRoms = sortRefObjectMaps(new ArrayList<>(allJoiningRoms));

        var refObjectMapPrefixes = new LinkedHashMap<RefObjectMap, String>();
        var leftJoins = new LinkedHashSet<LogicalViewJoin>();
        var aggregatingJoins = new LinkedHashSet<LogicalViewJoin>();

        for (int i = 0; i < sortedRoms.size(); i++) {
            var rom = sortedRoms.get(i);
            var prefix = "_ref%d.".formatted(i);
            refObjectMapPrefixes.put(rom, prefix);

            var parentTriplesMap = rom.getParentTriplesMap();
            var parentLogicalSource = parentTriplesMap.getLogicalSource();

            // Collect all parent expressions needed in the parent view:
            // 1. Parent SubjectMap reference expressions (for generating parent subjects)
            // 2. Join condition parent expressions (for join matching)
            var allParentExpressions = new LinkedHashSet<String>();
            parentTriplesMap.getSubjectMaps().stream()
                    .flatMap(sm -> sm.getExpressionMapExpressionSet().stream())
                    .sorted()
                    .forEach(allParentExpressions::add);
            rom.getJoinConditions().stream()
                    .flatMap(join -> join.getParentMap().getExpressionMapExpressionSet().stream())
                    .sorted()
                    .forEach(allParentExpressions::add);

            // Build parent view fields from all required parent expressions
            Set<Field> parentFields = allParentExpressions.stream()
                    .map(expression -> (Field) CarmlExpressionField.builder()
                            .fieldName(expression)
                            .reference(expression)
                            .build())
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            // Build synthetic parent LogicalView
            var parentView = CarmlLogicalView.builder()
                    .viewOn(parentLogicalSource)
                    .fields(parentFields)
                    .build();

            // Build join data fields (prefixed field names referencing parent expressions).
            // Only include SubjectMap expressions; join condition parent expressions are used
            // for matching but not exposed as prefixed data fields.
            var parentSubjectExpressions = parentTriplesMap.getSubjectMaps().stream()
                    .flatMap(sm -> sm.getExpressionMapExpressionSet().stream())
                    .sorted()
                    .toList();
            var joinFields = parentSubjectExpressions.stream()
                    .map(expression -> CarmlExpressionField.builder()
                            .fieldName(prefix + expression)
                            .reference(expression)
                            .build())
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            var leftJoin = CarmlLogicalViewJoin.builder()
                    .parentLogicalView(parentView)
                    .joinConditions(rom.getJoinConditions())
                    .fields(joinFields)
                    .build();

            leftJoins.add(leftJoin);
            if (aggregatingRoms.contains(rom)) {
                aggregatingJoins.add(leftJoin);
            }
        }

        var view = CarmlLogicalView.builder()
                .viewOn(logicalSource)
                .fields(fields)
                .leftJoins(leftJoins)
                .build();

        return new WrapResult(view, Map.copyOf(refObjectMapPrefixes), Set.copyOf(aggregatingJoins));
    }

    /**
     * Collects all expressions that need to be fields in the synthetic view. This includes the
     * TriplesMap's own reference expressions plus parent SubjectMap expressions from joinless
     * RefObjectMaps (same LogicalSource, no join conditions or self-joining), since those parent
     * expressions are resolved from the same data source row.
     *
     * <p>Gather expression references (from {@code rml:gather}) are excluded because they must
     * retain multi-valued semantics. View fields would either be unnested (splitting them into
     * separate rows) or flattened (losing the collection structure). These expressions are instead
     * evaluated via the source-level expression evaluation fallback.
     */
    private static Set<Field> collectViewFields(TriplesMap triplesMap) {
        var allExpressions = new LinkedHashSet<>(triplesMap.getReferenceExpressionSet());
        allExpressions.removeAll(collectGatherExpressions(triplesMap));
        collectJoinlessParentExpressions(triplesMap, allExpressions);
        // Include join condition child expressions from gather-based RefObjectMaps so they are
        // available as view fields for join matching. These are not collected by TriplesMap's
        // getReferenceExpressionSet() because the ROM is nested inside a gather map.
        collectGatherJoinChildExpressions(triplesMap, allExpressions);

        return allExpressions.stream()
                .map(expression -> (Field) CarmlExpressionField.builder()
                        .fieldName(expression)
                        .reference(expression)
                        .build())
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Collects all reference expressions that originate from gathered ObjectMaps. These expressions
     * must not be included as view fields because they require multi-valued evaluation (returning
     * all values at once) for container/collection generation.
     */
    private static Set<String> collectGatherExpressions(TriplesMap triplesMap) {
        return streamGatherItems(triplesMap)
                .filter(ObjectMap.class::isInstance)
                .map(ObjectMap.class::cast)
                .map(ObjectMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Collects parent SubjectMap reference expressions from joinless RefObjectMaps (those with empty
     * join conditions or self-joining). These expressions need to be included in the child's
     * synthetic view because the parent's subject is generated from the same data source row.
     */
    private static void collectJoinlessParentExpressions(TriplesMap triplesMap, Set<String> allExpressions) {
        if (!(triplesMap.getLogicalSource() instanceof LogicalSource)) {
            return;
        }
        for (var pom : triplesMap.getPredicateObjectMaps()) {
            for (var objectMap : pom.getObjectMaps()) {
                if (objectMap instanceof RefObjectMap rom
                        && (rom.getJoinConditions().isEmpty() || rom.isSelfJoining(triplesMap))) {
                    rom.getParentTriplesMap().getSubjectMaps().stream()
                            .flatMap(sm -> sm.getExpressionMapExpressionSet().stream())
                            .forEach(allExpressions::add);
                }
            }
        }
    }

    /**
     * Collects join condition child expressions from RefObjectMaps nested inside gather maps. These
     * expressions must be included as view fields so they are available in the evaluated values map
     * for join key matching. They are not collected by {@link TriplesMap#getReferenceExpressionSet()}
     * because the ROM is nested inside a gather, not a direct objectMap of the POM.
     */
    private static void collectGatherJoinChildExpressions(TriplesMap triplesMap, Set<String> allExpressions) {
        streamGatherRefObjectMaps(triplesMap)
                .flatMap(rom -> rom.getJoinConditions().stream())
                .flatMap(join -> join.getChildMap().getExpressionMapExpressionSet().stream())
                .forEach(allExpressions::add);
    }

    private static List<RefObjectMap> collectJoiningRefObjectMaps(TriplesMap triplesMap) {
        var result = new ArrayList<RefObjectMap>();
        for (var pom : triplesMap.getPredicateObjectMaps()) {
            for (var objectMap : pom.getObjectMaps()) {
                if (objectMap instanceof RefObjectMap rom
                        && !rom.getJoinConditions().isEmpty()
                        && !rom.isSelfJoining(triplesMap)) {
                    result.add(rom);
                }
            }
        }

        return result;
    }

    /**
     * Collects RefObjectMaps from {@code rml:gather} that have non-empty join conditions. These
     * require aggregating left joins: all matching parent values are collected into a list per child
     * row. Self-joining ROMs are NOT filtered out here because gather semantics require cross-row
     * resolution even for self-joins.
     */
    private static List<RefObjectMap> collectGatherJoiningRefObjectMaps(TriplesMap triplesMap) {
        return streamGatherRefObjectMaps(triplesMap)
                .filter(rom -> !rom.getJoinConditions().isEmpty())
                .toList();
    }

    /**
     * Streams all {@link RefObjectMap} instances nested inside {@code rml:gather} lists across all
     * predicate-object maps of the given TriplesMap.
     */
    private static Stream<RefObjectMap> streamGatherRefObjectMaps(TriplesMap triplesMap) {
        return streamGatherItems(triplesMap)
                .filter(RefObjectMap.class::isInstance)
                .map(RefObjectMap.class::cast);
    }

    /**
     * Streams all gather items (both {@link ObjectMap} and {@link RefObjectMap}) from
     * {@code rml:gather} lists across all predicate-object maps of the given TriplesMap.
     */
    private static Stream<BaseObjectMap> streamGatherItems(TriplesMap triplesMap) {
        return triplesMap.getPredicateObjectMaps().stream()
                .flatMap(pom -> pom.getObjectMaps().stream())
                .filter(ObjectMap.class::isInstance)
                .map(ObjectMap.class::cast)
                .flatMap(om -> om.getGathers().stream());
    }

    /**
     * Sorts RefObjectMaps deterministically by parent TriplesMap resource name, then by sorted join
     * condition string representation.
     */
    private static List<RefObjectMap> sortRefObjectMaps(List<RefObjectMap> roms) {
        return roms.stream()
                .sorted(Comparator.<RefObjectMap, String>comparing(
                                rom -> rom.getParentTriplesMap().getResourceName())
                        .thenComparing(rom -> rom.getJoinConditions().stream()
                                .map(join -> "%s=%s"
                                        .formatted(
                                                join.getChildMap().getExpressionMapExpressionSet(),
                                                join.getParentMap().getExpressionMapExpressionSet()))
                                .sorted()
                                .collect(Collectors.joining(","))))
                .toList();
    }
}
