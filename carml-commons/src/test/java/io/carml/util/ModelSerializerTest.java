package io.carml.util;

import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

class ModelSerializerTest {

    @Test
    void givenModel_whenSerializeAsRdf_thenReturnExpectedSerialization() {
        // Given
        var model = new ModelBuilder()
                .subject("http://example.org/subject")
                .add(RDF.TYPE, OWL.CLASS)
                .add(RDFS.LABEL, "foo")
                .build();

        // When
        var result = ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE);

        // Then
        assertThat(
                result,
                equalToCompressingWhiteSpace(" <http://example.org/subject> a <http://www.w3.org/2002/07/owl#Class>;"
                        + " <http://www.w3.org/2000/01/rdf-schema#label>  \"foo\" . "));
    }

    @Test
    void givenModelAndNamespaceApplier_whenSerializeAsRdf_thenReturnPrefixedSerialization() {
        // Given
        var model = new ModelBuilder()
                .subject("http://example.org/subject")
                .add(RDF.TYPE, OWL.CLASS)
                .add(RDFS.LABEL, "foo")
                .build();

        UnaryOperator<Model> namespaceApplier = mdl -> {
            mdl.setNamespace(OWL.NS);
            mdl.setNamespace(RDFS.NS);
            return mdl;
        };

        // When
        var result = ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE, namespaceApplier);

        // Then
        assertThat(
                result,
                equalToCompressingWhiteSpace(
                        "@prefix owl: <http://www.w3.org/2002/07/owl#> ." //
                                + " @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ." //
                                + " <http://example.org/subject> a owl:Class;" //
                                + " rdfs:label  \"foo\" . "));
    }

    @Test
    void given_whenFormatResourceForLog_then() {
        // Given
        var subject = iri("http://example.org/subject");
        var model = new ModelBuilder()
                .subject(subject)
                .add(RDF.TYPE, OWL.CLASS)
                .add(RDFS.LABEL, "foo")
                .build();

        // When
        var logString = ModelSerializer.formatResourceForLog(model, subject, Set.of(OWL.NS, RDFS.NS), false);

        // Then
        assertThat(logString, is("resource <http://example.org/subject>"));
    }

    @Test
    void givenBlankNodeResource_whenFormatResourceForLog_then() {
        // Given
        var subject = iri("http://example.org/subject");
        var related = bnode("bar");
        var model = new ModelBuilder()
                .subject(subject)
                .add(RDF.TYPE, OWL.CLASS)
                .add(RDFS.LABEL, "foo")
                .add(RDFS.SEEALSO, related)
                .subject(related)
                .add(RDFS.LABEL, "bar")
                .build();

        // When
        var logString = ModelSerializer.formatResourceForLog(model, related, Set.of(OWL.NS, RDFS.NS), false);

        // Then
        assertThat(
                logString,
                equalToCompressingWhiteSpace(
                        "blank node resource _:bar in: " //
                                + "```" //
                                + " <http://example.org/subject> rdfs:seeAlso [" //
                                + " rdfs:label \"bar\"" //
                                + " ] . " //
                                + "```"));
    }

    @Test
    void givenBlankNodeResourceAndCausedException_whenFormatResourceForLog_then() {
        // Given
        var subject = iri("http://example.org/subject");
        var related = bnode("bar");
        var model = new ModelBuilder()
                .subject(subject)
                .add(RDF.TYPE, OWL.CLASS)
                .add(RDFS.LABEL, "foo")
                .add(RDFS.SEEALSO, related)
                .subject(related)
                .add(RDFS.LABEL, "bar")
                .build();

        // When
        var logString = ModelSerializer.formatResourceForLog(model, related, Set.of(OWL.NS, RDFS.NS), true);

        // Then
        assertThat(
                logString,
                equalToCompressingWhiteSpace(
                        "blank node resource _:bar in: " //
                                + "```" //
                                + " <http://example.org/subject> rdfs:seeAlso [" //
                                + " :causedException \"<<-<<-<<-<<-<<\";" //
                                + " rdfs:label \"bar\"" //
                                + " ] . " //
                                + "```"));
    }
}
