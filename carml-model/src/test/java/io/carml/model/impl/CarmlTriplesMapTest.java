package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;

import io.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.jupiter.api.Test;

class CarmlTriplesMapTest {

    @Test
    void givenTriplesMapWithBaseIri_whenAddTriples_thenModelContainsBaseIriTriple() {
        // Given
        var baseIri = iri("http://override.example/");
        var triplesMap = CarmlTriplesMap.builder()
                .id("http://example.com/TriplesMap1")
                .baseIri(baseIri)
                .build();

        // When
        var modelBuilder = new ModelBuilder();
        triplesMap.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var expectedStatement = Statements.statement(
                Values.iri("http://example.com/TriplesMap1"),
                Rdf.Rml.baseIRI,
                Values.iri("http://override.example/"),
                null);

        assertThat(model, hasItem(expectedStatement));
    }

    @Test
    void givenTriplesMapWithoutBaseIri_whenAddTriples_thenModelDoesNotContainBaseIriTriple() {
        // Given
        var triplesMap =
                CarmlTriplesMap.builder().id("http://example.com/TriplesMap2").build();

        // When
        var modelBuilder = new ModelBuilder();
        triplesMap.addTriples(modelBuilder);
        Model model = modelBuilder.build();

        // Then
        var baseIriStatements = model.filter(null, Rdf.Rml.baseIRI, null);
        assertThat(baseIriStatements.isEmpty(), is(true));
    }
}
