package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Round-trip tests that serialize a known RDF graph with the Rio serializers, parse the output
 * back with Rio, and verify isomorphism. Exercises every supported format in both
 * {@link SerializerMode#STREAMING} and {@link SerializerMode#PRETTY} modes.
 */
class RioSerializerRoundTripTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static Model sampleTriples() {
        var model = new LinkedHashModel();
        var s1 = VF.createIRI("http://example.org/s1");
        var s2 = VF.createIRI("http://example.org/s2");
        var thing = VF.createIRI("http://example.org/Thing");
        model.add(s1, RDF.TYPE, thing);
        model.add(s1, RDFS.LABEL, VF.createLiteral("hello"));
        model.add(s1, RDFS.COMMENT, VF.createLiteral("héllo wörld"));
        model.add(s2, RDF.TYPE, thing);
        model.add(s2, RDFS.LABEL, VF.createLiteral("bye", "en"));
        return model;
    }

    private static Model sampleQuads() {
        var model = new LinkedHashModel();
        var s = VF.createIRI("http://example.org/s");
        var g1 = VF.createIRI("http://example.org/g1");
        var g2 = VF.createIRI("http://example.org/g2");
        model.add(s, RDF.TYPE, VF.createIRI("http://example.org/Thing"), g1);
        model.add(s, RDFS.LABEL, VF.createLiteral("in g1"), g1);
        model.add(s, RDFS.COMMENT, VF.createLiteral("in g2"), g2);
        return model;
    }

    // ---- triples formats round-trip in both modes ----

    private static Model tripleFormatsStreamingAndParseBack(String alias, RDFFormat format, SerializerMode mode) {
        var source = sampleTriples();
        var output = new ByteArrayOutputStream();

        RdfSerializer serializer =
                switch (mode) {
                    case STREAMING -> new RioStreamingSerializer(format);
                    case PRETTY -> new RioModelSerializer(format);
                    case BYTE_LEVEL -> throw new IllegalStateException("unreachable");
                };

        try (serializer) {
            serializer.start(output, Map.of("ex", "http://example.org/", "rdfs", RDFS.NAMESPACE));
            source.forEach(serializer::write);
            serializer.end();
        }

        try (var in = new ByteArrayInputStream(output.toByteArray())) {
            return Rio.parse(in, "", format);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse back %s output for %s: %s".formatted(format, alias, e.getMessage()), e);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_turtle_isIsomorphic(SerializerMode mode) {
        var parsed = tripleFormatsStreamingAndParseBack("ttl", RDFFormat.TURTLE, mode);
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_ntriples_isIsomorphic(SerializerMode mode) {
        var parsed = tripleFormatsStreamingAndParseBack("nt", RDFFormat.NTRIPLES, mode);
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_rdfxml_isIsomorphic(SerializerMode mode) {
        var parsed = tripleFormatsStreamingAndParseBack("rdfxml", RDFFormat.RDFXML, mode);
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_jsonld_isIsomorphic(SerializerMode mode) {
        var parsed = tripleFormatsStreamingAndParseBack("jsonld", RDFFormat.JSONLD, mode);
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    // ---- quad formats round-trip in both modes ----

    private static Model quadFormatsStreamingAndParseBack(String alias, RDFFormat format, SerializerMode mode) {
        var source = sampleQuads();
        var output = new ByteArrayOutputStream();

        RdfSerializer serializer =
                switch (mode) {
                    case STREAMING -> new RioStreamingSerializer(format);
                    case PRETTY -> new RioModelSerializer(format);
                    case BYTE_LEVEL -> throw new IllegalStateException("unreachable");
                };

        try (serializer) {
            serializer.start(output, Map.of("ex", "http://example.org/"));
            source.forEach(serializer::write);
            serializer.end();
        }

        try (var in = new ByteArrayInputStream(output.toByteArray())) {
            return Rio.parse(in, "", format);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse back %s output for %s: %s".formatted(format, alias, e.getMessage()), e);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_nquads_isIsomorphic(SerializerMode mode) {
        var parsed = quadFormatsStreamingAndParseBack("nq", RDFFormat.NQUADS, mode);
        assertThat(Models.isomorphic(parsed, sampleQuads()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_trig_isIsomorphic(SerializerMode mode) {
        var parsed = quadFormatsStreamingAndParseBack("trig", RDFFormat.TRIG, mode);
        assertThat(Models.isomorphic(parsed, sampleQuads()), is(true));
    }

    // ---- namespace handling in pretty mode ----

    @Test
    void prettyTurtle_emitsPrefixDeclarations() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral("hi"));

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
            serializer.start(output, Map.of("ex", "http://example.org/", "rdfs", RDFS.NAMESPACE));
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString();
        assertThat(result, containsString("@prefix ex:"));
        assertThat(result, containsString("@prefix rdfs:"));
    }
}
