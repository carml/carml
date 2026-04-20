package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import io.carml.model.LogicalTarget;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code TriplesMap#getAllLogicalTargets()} — covers every term-map location that can
 * declare an {@code rml:logicalTarget}. Each case verifies the default-method traversal returns
 * exactly the expected singleton (or union) of declared targets.
 */
class CarmlTriplesMapLogicalTargetTest {

    /**
     * Builds a distinct {@link LogicalTarget} per call by using a unique serialization IRI.
     * {@code CarmlLogicalTarget} equality is derived from its {@code target} field, and
     * {@code CarmlTarget} equality is derived from its serialization / encoding / compression
     * fields — the resource {@code id} is excluded. Using distinct serialization IRIs is the
     * cleanest way to make each target unique for set-membership assertions.
     */
    private static LogicalTarget target(String id) {
        return CarmlLogicalTarget.builder()
                .target(CarmlTarget.builder()
                        .serialization(iri("http://example.com/serialization/" + id))
                        .build())
                .id(id)
                .build();
    }

    @Test
    void getAllLogicalTargets_targetOnSubjectMap_returnsSingleton() {
        // Given
        var target = target("http://example.com/target/subject");
        var subjectMap = CarmlSubjectMap.builder()
                .reference("id")
                .logicalTargets(Set.of(target))
                .build();
        var triplesMap = CarmlTriplesMap.builder().subjectMap(subjectMap).build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, containsInAnyOrder(target));
    }

    @Test
    void getAllLogicalTargets_targetOnSubjectMapGraphMap_returnsSingleton() {
        // Given
        var target = target("http://example.com/target/subject-graph");
        var graphMap = CarmlGraphMap.builder()
                .constant(iri("http://example.com/graph"))
                .logicalTargets(Set.of(target))
                .build();
        var subjectMap =
                CarmlSubjectMap.builder().reference("id").graphMap(graphMap).build();
        var triplesMap = CarmlTriplesMap.builder().subjectMap(subjectMap).build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, containsInAnyOrder(target));
    }

    @Test
    void getAllLogicalTargets_targetOnPredicateMap_returnsSingleton() {
        // Given
        var target = target("http://example.com/target/predicate");
        var predicateMap = CarmlPredicateMap.builder()
                .constant(iri("http://example.com/pred"))
                .logicalTargets(Set.of(target))
                .build();
        var objectMap = CarmlObjectMap.builder().reference("o").build();
        var pom = CarmlPredicateObjectMap.builder()
                .predicateMap(predicateMap)
                .objectMap(objectMap)
                .build();
        var subjectMap = CarmlSubjectMap.builder().reference("id").build();
        var triplesMap = CarmlTriplesMap.builder()
                .subjectMap(subjectMap)
                .predicateObjectMap(pom)
                .build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, containsInAnyOrder(target));
    }

    @Test
    void getAllLogicalTargets_targetOnObjectMap_returnsSingleton() {
        // Given
        var target = target("http://example.com/target/object");
        var predicateMap = CarmlPredicateMap.builder()
                .constant(iri("http://example.com/pred"))
                .build();
        var objectMap = CarmlObjectMap.builder()
                .reference("o")
                .logicalTargets(Set.of(target))
                .build();
        var pom = CarmlPredicateObjectMap.builder()
                .predicateMap(predicateMap)
                .objectMap(objectMap)
                .build();
        var subjectMap = CarmlSubjectMap.builder().reference("id").build();
        var triplesMap = CarmlTriplesMap.builder()
                .subjectMap(subjectMap)
                .predicateObjectMap(pom)
                .build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, containsInAnyOrder(target));
    }

    @Test
    void getAllLogicalTargets_targetOnPomGraphMap_returnsSingleton() {
        // Given
        var target = target("http://example.com/target/pom-graph");
        var graphMap = CarmlGraphMap.builder()
                .constant(iri("http://example.com/graph"))
                .logicalTargets(Set.of(target))
                .build();
        var predicateMap = CarmlPredicateMap.builder()
                .constant(iri("http://example.com/pred"))
                .build();
        var objectMap = CarmlObjectMap.builder().reference("o").build();
        var pom = CarmlPredicateObjectMap.builder()
                .predicateMap(predicateMap)
                .objectMap(objectMap)
                .graphMap(graphMap)
                .build();
        var subjectMap = CarmlSubjectMap.builder().reference("id").build();
        var triplesMap = CarmlTriplesMap.builder()
                .subjectMap(subjectMap)
                .predicateObjectMap(pom)
                .build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, containsInAnyOrder(target));
    }

    @Test
    void getAllLogicalTargets_targetsOnAllTermMapLocations_returnsUnion() {
        // Given — one target on each of the five locations covered by the traversal:
        // subject map, subject-map graph map, predicate map, object map, POM graph map.
        var subjectTarget = target("http://example.com/target/subject");
        var subjectGraphTarget = target("http://example.com/target/subject-graph");
        var predicateTarget = target("http://example.com/target/predicate");
        var objectTarget = target("http://example.com/target/object");
        var pomGraphTarget = target("http://example.com/target/pom-graph");

        var subjectGraphMap = CarmlGraphMap.builder()
                .constant(iri("http://example.com/g1"))
                .logicalTargets(Set.of(subjectGraphTarget))
                .build();
        var subjectMap = CarmlSubjectMap.builder()
                .reference("id")
                .graphMap(subjectGraphMap)
                .logicalTargets(Set.of(subjectTarget))
                .build();

        var predicateMap = CarmlPredicateMap.builder()
                .constant(iri("http://example.com/pred"))
                .logicalTargets(Set.of(predicateTarget))
                .build();
        var objectMap = CarmlObjectMap.builder()
                .reference("o")
                .logicalTargets(Set.of(objectTarget))
                .build();
        var pomGraphMap = CarmlGraphMap.builder()
                .constant(iri("http://example.com/g2"))
                .logicalTargets(Set.of(pomGraphTarget))
                .build();
        var pom = CarmlPredicateObjectMap.builder()
                .predicateMap(predicateMap)
                .objectMap(objectMap)
                .graphMap(pomGraphMap)
                .build();

        var triplesMap = CarmlTriplesMap.builder()
                .subjectMap(subjectMap)
                .predicateObjectMap(pom)
                .build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(
                collected,
                containsInAnyOrder(subjectTarget, subjectGraphTarget, predicateTarget, objectTarget, pomGraphTarget));
    }

    @Test
    void getAllLogicalTargets_noTargetsDeclared_returnsEmpty() {
        // Given
        var subjectMap = CarmlSubjectMap.builder().reference("id").build();
        var triplesMap = CarmlTriplesMap.builder().subjectMap(subjectMap).build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then
        assertThat(collected, empty());
    }

    @Test
    void getAllLogicalTargets_withSameTargetDeclaredMultipleTimes_returnsOnce() {
        // Given - the same LogicalTarget instance is declared on the SubjectMap and on an
        // ObjectMap. The traversal must dedupe so a single physical target writer is created
        // downstream regardless of how many term maps reference it. This pins the contract
        // CarmlMapCommand.collectLogicalTargets relies on (the union returned to the router
        // factory must contain each target at most once).
        var sharedTarget = target("http://example.com/target/shared");

        var subjectMap = CarmlSubjectMap.builder()
                .reference("id")
                .logicalTargets(Set.of(sharedTarget))
                .build();
        var predicateMap = CarmlPredicateMap.builder()
                .constant(iri("http://example.com/pred"))
                .build();
        var objectMap = CarmlObjectMap.builder()
                .reference("o")
                .logicalTargets(Set.of(sharedTarget))
                .build();
        var pom = CarmlPredicateObjectMap.builder()
                .predicateMap(predicateMap)
                .objectMap(objectMap)
                .build();
        var triplesMap = CarmlTriplesMap.builder()
                .subjectMap(subjectMap)
                .predicateObjectMap(pom)
                .build();

        // When
        var collected = triplesMap.getAllLogicalTargets();

        // Then - the shared target appears exactly once in the union.
        assertThat(collected, hasSize(1));
        assertThat(collected, containsInAnyOrder(sharedTarget));
    }
}
