package io.carml.logicalview;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.model.LogicalView;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Decomposes a {@link TriplesMap} into groups of {@link PredicateObjectMap}s based on field
 * dependencies derived from the associated {@link LogicalView}'s structural annotations.
 *
 * <p>When a TriplesMap references fields with partial dependencies (some fields determined by a
 * subset of the primary key), evaluating all rows against all POMs produces duplicate triples for
 * over-determined fields. Decomposition groups POMs by their effective determinant, allowing each
 * group to be evaluated with a narrower projection and DISTINCT, eliminating duplicates at the
 * source.
 */
@Slf4j
public final class ViewDecomposer {

    private ViewDecomposer() {}

    /**
     * A group of predicate-object maps that share the same effective determinant and can be
     * evaluated together with a common field projection.
     *
     * @param predicateObjectMaps the POMs in this group
     * @param projectedFields the field names needed for evaluating this group (subject fields +
     *     determinant + POM-referenced fields)
     * @param emitsClassTriples whether this group should emit rdf:type class triples for the subject
     */
    public record DecompositionGroup(
            Set<PredicateObjectMap> predicateObjectMaps, Set<String> projectedFields, boolean emitsClassTriples) {}

    /**
     * The result of decomposing a TriplesMap.
     *
     * @param groups the decomposition groups (one or more)
     * @param isDecomposed {@code true} if the TriplesMap was split into multiple groups; {@code
     *     false} if decomposition was not applicable or yielded a single group
     */
    public record DecompositionResult(List<DecompositionGroup> groups, boolean isDecomposed) {

        /**
         * Creates a single-group result representing no decomposition.
         */
        public static DecompositionResult single(Set<PredicateObjectMap> allPoms, Set<String> allFields) {
            return new DecompositionResult(List.of(new DecompositionGroup(allPoms, allFields, true)), false);
        }
    }

    /**
     * Decomposes the given TriplesMap into groups based on field dependencies from the logical view.
     *
     * <p>If no functional dependencies exist, or if all POMs share the same effective determinant,
     * returns a single-group result with {@code isDecomposed=false}.
     *
     * @param triplesMap the triples map to decompose
     * @param view the logical view providing structural annotations
     * @return the decomposition result
     */
    public static DecompositionResult decompose(TriplesMap triplesMap, LogicalView view) {
        var allPoms = triplesMap.getPredicateObjectMaps();
        var allFields = triplesMap.getReferenceExpressionSet();

        var dependencies = FieldDependencyResolver.resolveDependencies(view);
        if (dependencies.isEmpty()) {
            return DecompositionResult.single(allPoms, allFields);
        }

        var candidateKeys = FieldDependencyResolver.collectCandidateKeys(view);
        if (candidateKeys.isEmpty()) {
            return DecompositionResult.single(allPoms, allFields);
        }

        var subjectFields = triplesMap.getSubjectMaps().stream()
                .flatMap(sm -> sm.getReferenceExpressionSet().stream())
                .collect(toUnmodifiableSet());

        var pomsByDeterminant = groupPomsByDeterminant(allPoms, candidateKeys, dependencies);

        if (pomsByDeterminant.size() <= 1) {
            return DecompositionResult.single(allPoms, allFields);
        }

        // Sort groups by determinant size (ascending) so the narrowest determinant group
        // is first and emits class triples. This is deterministic regardless of POM iteration order.
        var sortedEntries = pomsByDeterminant.entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getKey().size()))
                .toList();

        var groups = new ArrayList<DecompositionGroup>(sortedEntries.size());
        for (var i = 0; i < sortedEntries.size(); i++) {
            var entry = sortedEntries.get(i);
            var determinant = entry.getKey();
            var poms = Set.copyOf(entry.getValue());
            var pomFieldsForGroup = triplesMap.getReferenceExpressionSet(poms);
            var projectedFields = new LinkedHashSet<>(subjectFields);
            projectedFields.addAll(determinant);
            projectedFields.addAll(pomFieldsForGroup);
            groups.add(new DecompositionGroup(poms, Set.copyOf(projectedFields), i == 0));
        }

        LOG.debug("Decomposed TriplesMap into {} groups based on field dependencies", groups.size());
        return new DecompositionResult(List.copyOf(groups), true);
    }

    private static LinkedHashMap<Set<String>, List<PredicateObjectMap>> groupPomsByDeterminant(
            Set<PredicateObjectMap> allPoms,
            Set<Set<String>> candidateKeys,
            Set<FieldDependencyResolver.FieldDependency> dependencies) {
        var pomsByDeterminant = new LinkedHashMap<Set<String>, List<PredicateObjectMap>>();

        var primaryKey = candidateKeys.iterator().next();

        for (var pom : allPoms) {
            if (hasJoinCondition(pom)) {
                pomsByDeterminant
                        .computeIfAbsent(primaryKey, k -> new ArrayList<>())
                        .add(pom);
                continue;
            }

            var pomFields = pom.getReferenceExpressionSet();
            var determinant = findMinimalDeterminant(pomFields, candidateKeys, dependencies);
            pomsByDeterminant
                    .computeIfAbsent(determinant, k -> new ArrayList<>())
                    .add(pom);
        }

        return pomsByDeterminant;
    }

    /**
     * Finds the minimal subset of any candidate key that transitively covers all target fields via
     * the given functional dependencies. Currently tries single-field subsets and the full key; does
     * not enumerate intermediate multi-field subsets. This is sufficient for FK-derived FDs where the
     * FK local fields are typically a single-field subset of the PK.
     */
    private static Set<String> findMinimalDeterminant(
            Set<String> targetFields,
            Set<Set<String>> candidateKeys,
            Set<FieldDependencyResolver.FieldDependency> dependencies) {
        return candidateKeys.stream()
                .flatMap(key -> candidatesForKey(key, targetFields, dependencies))
                .min(Comparator.comparingInt(Set::size))
                .orElseThrow(() ->
                        new IllegalStateException("No candidate key covers target fields %s".formatted(targetFields)));
    }

    private static Stream<Set<String>> candidatesForKey(
            Set<String> key, Set<String> targetFields, Set<FieldDependencyResolver.FieldDependency> dependencies) {
        // Try each single field as a potential minimal determinant
        var singleFieldCandidates =
                key.stream().map(Set::of).filter(subset -> coversTarget(subset, targetFields, dependencies));

        // Also try the full key
        var fullKeyCandidates =
                coversTarget(key, targetFields, dependencies) ? Stream.of(key) : Stream.<Set<String>>empty();

        return Stream.concat(singleFieldCandidates, fullKeyCandidates);
    }

    private static boolean coversTarget(
            Set<String> subset, Set<String> targetFields, Set<FieldDependencyResolver.FieldDependency> dependencies) {
        return FieldDependencyResolver.resolveTransitiveCoverage(subset, dependencies)
                .containsAll(targetFields);
    }

    private static boolean hasJoinCondition(PredicateObjectMap pom) {
        return pom.getObjectMaps().stream()
                .anyMatch(om -> om instanceof RefObjectMap rom
                        && !rom.getJoinConditions().isEmpty());
    }
}
