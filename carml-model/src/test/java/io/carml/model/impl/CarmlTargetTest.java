package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

import io.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;

class CarmlTargetTest {

    @Test
    void givenTargetWithSerialization_whenGetSerialization_thenReturnValue() {
        // Given
        var serialization = iri("http://www.w3.org/ns/formats/N-Triples");
        var target = CarmlTarget.builder().serialization(serialization).build();

        // When
        var result = target.getSerialization();

        // Then
        assertThat(result, is(serialization));
    }

    @Test
    void givenTargetWithoutSerialization_whenGetSerialization_thenReturnNull() {
        // Given
        var target = CarmlTarget.builder().build();

        // When
        var result = target.getSerialization();

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    void givenTargetWithEncoding_whenGetEncoding_thenReturnValue() {
        // Given
        var encoding = iri("http://example.com/utf-8");
        var target = CarmlTarget.builder().encoding(encoding).build();

        // When
        var result = target.getEncoding();

        // Then
        assertThat(result, is(encoding));
    }

    @Test
    void givenTargetWithoutEncoding_whenGetEncoding_thenReturnNull() {
        // Given
        var target = CarmlTarget.builder().build();

        // When
        var result = target.getEncoding();

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    void givenTargetWithCompression_whenGetCompression_thenReturnValue() {
        // Given
        var compression = iri("http://example.com/gzip");
        var target = CarmlTarget.builder().compression(compression).build();

        // When
        var result = target.getCompression();

        // Then
        assertThat(result, is(compression));
    }

    @Test
    void givenTargetWithoutCompression_whenGetCompression_thenReturnNull() {
        // Given
        var target = CarmlTarget.builder().build();

        // When
        var result = target.getCompression();

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    void givenTargetWithAllFields_whenAddTriples_thenModelContainsExpectedTriples() {
        // Given
        var serialization = iri("http://www.w3.org/ns/formats/N-Triples");
        var encoding = iri("http://example.com/utf-8");
        var compression = iri("http://example.com/gzip");
        var target = CarmlTarget.builder()
                .id("http://example.com/Target1")
                .serialization(serialization)
                .encoding(encoding)
                .compression(compression)
                .build();

        // When
        var modelBuilder = new ModelBuilder();
        target.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var subject = iri("http://example.com/Target1");

        assertThat(model, hasItem(Statements.statement(subject, RDF.TYPE, Rdf.Rml.Target, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.serialization, serialization, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.encoding, encoding, null)));
        assertThat(model, hasItem(Statements.statement(subject, Rdf.Rml.compression, compression, null)));
    }

    @Test
    void givenTargetWithNoFields_whenAddTriples_thenModelContainsOnlyTypeTriple() {
        // Given
        var target = CarmlTarget.builder().id("http://example.com/Target2").build();

        // When
        var modelBuilder = new ModelBuilder();
        target.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var subject = iri("http://example.com/Target2");

        assertThat(model, hasItem(Statements.statement(subject, RDF.TYPE, Rdf.Rml.Target, null)));
        assertThat(model.filter(null, Rdf.Rml.serialization, null).isEmpty(), is(true));
        assertThat(model.filter(null, Rdf.Rml.encoding, null).isEmpty(), is(true));
        assertThat(model.filter(null, Rdf.Rml.compression, null).isEmpty(), is(true));
    }
}
