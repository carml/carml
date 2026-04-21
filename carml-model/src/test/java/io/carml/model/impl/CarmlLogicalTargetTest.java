package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;

/**
 * Covers the new {@code rml:serialization}/{@code rml:encoding}/{@code rml:compression}
 * properties on {@link CarmlLogicalTarget}. Per RML-IO these are primarily properties of
 * {@code rml:LogicalTarget}; the getters default to {@code null} when not set and the
 * {@code addTriples} method only emits them when non-null so unused properties do not pollute
 * round-tripped RDF.
 */
class CarmlLogicalTargetTest {

    @Test
    void givenLogicalTargetWithoutProperties_whenGetters_thenReturnNull() {
        // Given — a bare LogicalTarget with only its nested target declared.
        var target = CarmlTarget.builder().build();
        var logicalTarget = CarmlLogicalTarget.builder().target(target).build();

        // Then — all three new getters default to null, letting the CLI routing layer decide
        // which level (LogicalTarget vs Target) supplies the effective value.
        assertThat(logicalTarget.getSerialization(), is(nullValue()));
        assertThat(logicalTarget.getEncoding(), is(nullValue()));
        assertThat(logicalTarget.getCompression(), is(nullValue()));
    }

    @Test
    void givenLogicalTargetWithSerialization_whenGetSerialization_thenReturnValue() {
        // Given
        var serialization = iri("http://www.w3.org/ns/formats/N-Quads");
        var logicalTarget =
                CarmlLogicalTarget.builder().serialization(serialization).build();

        // When / Then
        assertThat(logicalTarget.getSerialization(), is(serialization));
    }

    @Test
    void givenLogicalTargetWithEncoding_whenGetEncoding_thenReturnValue() {
        // Given
        var encoding = iri("http://w3id.org/rml/UTF-16");
        var logicalTarget = CarmlLogicalTarget.builder().encoding(encoding).build();

        // When / Then
        assertThat(logicalTarget.getEncoding(), is(encoding));
    }

    @Test
    void givenLogicalTargetWithCompression_whenGetCompression_thenReturnValue() {
        // Given
        var compression = iri("http://w3id.org/rml/gzip");
        var logicalTarget =
                CarmlLogicalTarget.builder().compression(compression).build();

        // When / Then
        assertThat(logicalTarget.getCompression(), is(compression));
    }

    @Test
    void givenLogicalTargetWithAllFields_whenAddTriples_thenModelContainsAllProperties() {
        // Given — serialization/encoding/compression on LogicalTarget (the RML-IO primary
        // location); the addTriples method must emit all three onto the LogicalTarget subject.
        var serialization = iri("http://www.w3.org/ns/formats/N-Triples");
        var encoding = iri("http://w3id.org/rml/UTF-16");
        var compression = iri("http://w3id.org/rml/gzip");
        var logicalTarget = CarmlLogicalTarget.builder()
                .id("http://example.com/LogicalTarget1")
                .serialization(serialization)
                .encoding(encoding)
                .compression(compression)
                .build();

        // When
        var modelBuilder = new ModelBuilder();
        logicalTarget.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var subject = iri("http://example.com/LogicalTarget1");
        assertThat(model, hasItem(Statements.statement(subject, RDF.TYPE, Rdf.Rml.LogicalTarget, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.serialization, serialization, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.encoding, encoding, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.compression, compression, null)));
    }

    @Test
    void givenLogicalTargetWithOnlySerialization_whenAddTriples_thenModelContainsOnlySerializationTriple() {
        // Given — exactly one of the three RML-IO properties (serialization) is set. addTriples
        // must emit ONLY that one property triple alongside the rdf:type; the other two must not
        // leak into the model. This pins the per-property null-check inside addTriples against a
        // regression that would either emit all three or none.
        var serialization = iri("http://www.w3.org/ns/formats/N-Triples");
        var logicalTarget = CarmlLogicalTarget.builder()
                .id("http://example.com/LT3")
                .serialization(serialization)
                .build();

        // When
        var modelBuilder = new ModelBuilder();
        logicalTarget.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var subject = iri("http://example.com/LT3");
        assertThat(model, hasItem(Statements.statement(subject, RDF.TYPE, Rdf.Rml.LogicalTarget, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.serialization, serialization, null)));
        // Exactly one serialization triple on the subject — the one we just set.
        assertThat(model.filter(null, Rdf.Rml.serialization, null).size(), is(1));
        // Encoding and compression MUST NOT appear anywhere in the serialized model.
        assertThat(model.filter(null, Rdf.Rml.encoding, null).isEmpty(), is(true));
        assertThat(model.filter(null, Rdf.Rml.compression, null).isEmpty(), is(true));
    }

    @Test
    void givenLogicalTargetWithNoProperties_whenAddTriples_thenModelContainsOnlyTypeTriple() {
        // Given — no optional properties set; addTriples must NOT emit any of the three RML-IO
        // properties (they are optional per the spec and would otherwise pollute the serialized
        // model).
        var logicalTarget =
                CarmlLogicalTarget.builder().id("http://example.com/LT2").build();

        // When
        var modelBuilder = new ModelBuilder();
        logicalTarget.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var subject = iri("http://example.com/LT2");
        assertThat(model, hasItem(Statements.statement(subject, RDF.TYPE, Rdf.Rml.LogicalTarget, null)));
        assertThat(model.filter(null, Rdf.Rml.serialization, null).isEmpty(), is(true));
        assertThat(model.filter(null, Rdf.Rml.encoding, null).isEmpty(), is(true));
        assertThat(model.filter(null, Rdf.Rml.compression, null).isEmpty(), is(true));
    }

    @Test
    void givenLogicalTargetWithTarget_whenGetReferencedResources_thenContainsTarget() {
        // Given
        var target = CarmlTarget.builder().id("http://example.com/T").build();
        var logicalTarget = CarmlLogicalTarget.builder().target(target).build();

        // Then — referenced resources include the nested target so RDF serialization walks into
        // it. Pinned here to guard against regressions as the model grows new fields.
        assertThat(logicalTarget.getReferencedResources(), contains(target));
    }
}
