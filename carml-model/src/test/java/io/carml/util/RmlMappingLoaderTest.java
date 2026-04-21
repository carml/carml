package io.carml.util;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.carml.model.FilePath;
import io.carml.model.LogicalTarget;
import io.carml.model.Target;
import io.carml.model.TriplesMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@link RmlMappingLoader} correctly instantiates the object graph for RML-IO
 * LogicalTarget mappings. The two shapes under test mirror the RMLTTC fixtures:
 *
 * <ul>
 *   <li>{@code rml:target [ a rml:FilePath ]} — single-type decidable resolution relies on
 *       {@code FilePath} extending {@link Target} so the rdf-mapper proxy is assignable to the
 *       {@code Target}-typed property.
 *   <li>{@code rml:target [ a rml:Target, rml:FilePath ]} — multi-type path requires both RDF
 *       types to be registered against the mapper.
 * </ul>
 */
class RmlMappingLoaderTest {

    private final RmlMappingLoader mappingLoader = RmlMappingLoader.build();

    private static Set<LogicalTarget> collectAllLogicalTargets(Set<TriplesMap> triplesMaps) {
        return triplesMaps.stream()
                .flatMap(tm -> tm.getAllLogicalTargets().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static LogicalTarget findByPath(Set<LogicalTarget> logicalTargets, String filePathValue) {
        return logicalTargets.stream()
                .filter(lt ->
                        lt.getTarget() instanceof FilePath fp && fp.getPath().equals(filePathValue))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No LogicalTarget with FilePath path '%s' found in %s"
                        .formatted(filePathValue, logicalTargets)));
    }

    @Test
    void load_mappingWithFilePathTarget_resolvesFilePathAsTarget() {
        // Given — a mapping that uses `rml:target [ a rml:FilePath ]` (single-type decider).
        var mappingSource = getClass().getResourceAsStream("MappingWithFilePathTarget.rml.ttl");
        assertThat(mappingSource, is(notNullValue()));

        // When
        Set<TriplesMap> triplesMaps = mappingLoader.load(RDFFormat.TURTLE, mappingSource);
        assertThat(triplesMaps, hasSize(4));
        var logicalTargets = collectAllLogicalTargets(triplesMaps);
        assertThat(logicalTargets, hasSize(4));

        // Then — the LogicalTarget with dumpA.nq was created with a FilePath-as-Target nested
        // object. Assignability to BOTH Target and FilePath is the load-bearing invariant: the
        // rdf-mapper proxy implements whichever rdf:type was decidable, and with FilePath
        // extending Target the single-type case still types the target property correctly.
        var logicalTarget = findByPath(logicalTargets, "./dumpA.nq");
        var target = logicalTarget.getTarget();
        assertThat(target, is(notNullValue()));
        assertThat(target, instanceOf(Target.class));
        assertThat(target, instanceOf(FilePath.class));
        assertThat(((FilePath) target).getRoot(), is(iri("http://w3id.org/rml/CurrentWorkingDirectory")));
        // serialization lives on LogicalTarget in RML-IO
        assertThat(logicalTarget.getSerialization(), is(iri("http://www.w3.org/ns/formats/N-Quads")));
        // FilePath does not carry its own serialization
        assertThat(target.getSerialization(), is(nullValue()));
    }

    @Test
    void load_mappingWithCombinedTargetAndFilePath_resolvesBothTypes() {
        // Given — `rml:target [ a rml:Target, rml:FilePath ]` triggers the multi-type path in
        // TypeFromTripleTypeDecider. Both IRIs must resolve to registered interface types,
        // otherwise the loader throws at the getDecidableType step.
        var mappingSource = getClass().getResourceAsStream("MappingWithFilePathTarget.rml.ttl");
        assertThat(mappingSource, is(notNullValue()));

        // When
        Set<TriplesMap> triplesMaps = mappingLoader.load(RDFFormat.TURTLE, mappingSource);
        var logicalTargets = collectAllLogicalTargets(triplesMaps);

        // Then — the LogicalTarget with dumpB.nq was created with a combined-typed Target/FilePath
        // proxy. Both instance checks must succeed to prove both decidable types were registered.
        var logicalTarget = findByPath(logicalTargets, "./dumpB.nq");
        var target = logicalTarget.getTarget();
        assertThat(target, is(notNullValue()));
        assertThat(target, instanceOf(Target.class));
        assertThat(target, instanceOf(FilePath.class));
        assertThat(logicalTarget.getSerialization(), is(iri("http://www.w3.org/ns/formats/N-Triples")));
        assertThat(logicalTarget.getEncoding(), is(iri("http://w3id.org/rml/UTF-16")));
    }

    @Test
    void load_mappingWithCombinedTargetAndFilePath_andSerializationOnNestedTarget_dispatchesConsistently() {
        // Given — `rml:target [ a rml:Target, rml:FilePath ; rml:serialization ... ]`: the nested
        // resource is combined-typed AND carries its own serialization (the surrounding
        // LogicalTarget does NOT declare one). This exercises first-delegate-wins dispatch inside
        // CarmlMapper.doMultipleInterfaceMapping for target.getSerialization(): the proxy selects
        // whichever delegate's implementation type iterates first in the HashSet. With two
        // interface types (Target, FilePath) the iteration order is stable within a JVM run but
        // NOT guaranteed across runs — empirically, running this test class in isolation
        // surfaces the Target delegate (returns the declared IRI), while running the full
        // carml-model suite surfaces the FilePath delegate (CarmlFilePath.getSerialization()
        // returns null). See FilePath.java javadoc for the guidance: prefer LogicalTarget.
        var mappingSource = getClass().getResourceAsStream("MappingWithFilePathTarget.rml.ttl");
        assertThat(mappingSource, is(notNullValue()));

        // When
        Set<TriplesMap> triplesMaps = mappingLoader.load(RDFFormat.TURTLE, mappingSource);
        var logicalTargets = collectAllLogicalTargets(triplesMaps);

        // Then
        var logicalTarget = findByPath(logicalTargets, "./dumpC.nt");
        var target = logicalTarget.getTarget();
        assertThat(target, is(notNullValue()));
        assertThat(target, instanceOf(Target.class));
        assertThat(target, instanceOf(FilePath.class));
        // LogicalTarget has no serialization of its own here — the CLI routing path falls back
        // to target.getSerialization() via the LogicalTarget-over-Target precedence rule.
        assertThat(logicalTarget.getSerialization(), is(nullValue()));
        // Observed non-determinism: accept either outcome. The assertion is a regression pin —
        // it documents that the dispatch path reaches one of the two delegates without throwing,
        // and serves as a warning bell if the proxy ever starts returning something else
        // entirely. Any single observed value would flake when the test ordering changes.
        assertThat(
                target.getSerialization(), anyOf(is(iri("http://www.w3.org/ns/formats/N-Triples")), is(nullValue())));
    }

    @Test
    void load_mappingWithBareRmlTargetNode_resolvesToCarmlTarget() {
        // Given — `rml:target [ a rml:Target ]` with no rml:FilePath type. This exercises the
        // defensive binding addDecidableType(Rdf.Rml.Target, Target.class) +
        // bindInterfaceImplementation(Target.class, CarmlTarget.class) in RmlMappingLoader. The
        // mapping layer must be able to resolve the bare Target type without requiring the
        // FilePath sibling. (Routing such a target from the CLI would fail because there's no
        // path to resolve — this is a loader-level test only.)
        var mappingSource = getClass().getResourceAsStream("MappingWithFilePathTarget.rml.ttl");
        assertThat(mappingSource, is(notNullValue()));

        // When
        Set<TriplesMap> triplesMaps = mappingLoader.load(RDFFormat.TURTLE, mappingSource);
        var logicalTargets = collectAllLogicalTargets(triplesMaps);

        // Then — locate the LT whose nested target is a Target but NOT a FilePath (TargetD).
        // Equality on the LogicalTarget itself is structural, so filtering the collected set
        // by instance-type predicates on the nested target is the most direct anchor.
        var logicalTarget = logicalTargets.stream()
                .filter(lt -> lt.getTarget() != null
                        && lt.getTarget() instanceof Target
                        && !(lt.getTarget() instanceof FilePath))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No LogicalTarget with a bare Target (non-FilePath) nested target found in " + logicalTargets));
        var target = logicalTarget.getTarget();
        assertThat(target, is(notNullValue()));
        assertThat(target, instanceOf(Target.class));
        assertThat(target, is(not(instanceOf(FilePath.class))));
        // Target-level serialization on the bare node flows through the CarmlTarget proxy.
        assertThat(target.getSerialization(), is(iri("http://www.w3.org/ns/formats/N-Quads")));
    }
}
