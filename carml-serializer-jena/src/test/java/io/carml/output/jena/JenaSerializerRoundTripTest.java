package io.carml.output.jena;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.carml.output.RdfSerializer;
import io.carml.output.SerializerMode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.jena.riot.Lang;
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
 * Round-trip tests that serialize a known RDF graph with the Jena serializers, parse the output
 * back with RDF4J Rio, and verify isomorphism. Exercises supported formats in both
 * {@link SerializerMode#STREAMING} and {@link SerializerMode#PRETTY} modes.
 */
class JenaSerializerRoundTripTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static Model sampleTriples() {
        var model = new LinkedHashModel();
        var s1 = VF.createIRI("http://example.org/s1");
        var s2 = VF.createIRI("http://example.org/s2");
        var thing = VF.createIRI("http://example.org/Thing");
        model.add(s1, RDF.TYPE, thing);
        model.add(s1, RDFS.LABEL, VF.createLiteral("hello"));
        model.add(s1, RDFS.COMMENT, VF.createLiteral("world"));
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

    // ---- triples format round-trips ----

    private static Model serializeAndParseBack(
            Lang jenaLang, RDFFormat rdf4jFormat, SerializerMode mode, Model source) {
        var output = new ByteArrayOutputStream();

        RdfSerializer serializer =
                switch (mode) {
                    case STREAMING -> new JenaStreamingSerializer(jenaLang);
                    case PRETTY -> new JenaModelSerializer(jenaLang);
                    case BYTE_LEVEL -> throw new IllegalStateException("unreachable");
                };

        try (serializer) {
            serializer.start(
                    output, Map.of("ex", "http://example.org/", "rdfs", "http://www.w3.org/2000/01/rdf-schema#"));
            source.forEach(serializer::write);
            serializer.end();
        }

        try (var in = new ByteArrayInputStream(output.toByteArray())) {
            return Rio.parse(in, "", rdf4jFormat);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse back %s output: %s%nOutput was:%n%s"
                            .formatted(jenaLang.getName(), e.getMessage(), output.toString()),
                    e);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_ntriples_isIsomorphic(SerializerMode mode) {
        var parsed = serializeAndParseBack(Lang.NTRIPLES, RDFFormat.NTRIPLES, mode, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_turtle_isIsomorphic(SerializerMode mode) {
        var parsed = serializeAndParseBack(Lang.TURTLE, RDFFormat.TURTLE, mode, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @Test
    void roundTrip_rdfxml_pretty_isIsomorphic() {
        var parsed = serializeAndParseBack(Lang.RDFXML, RDFFormat.RDFXML, SerializerMode.PRETTY, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @Test
    void roundTrip_jsonld_pretty_isIsomorphic() {
        var parsed = serializeAndParseBack(Lang.JSONLD, RDFFormat.JSONLD, SerializerMode.PRETTY, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_trix_isIsomorphic(SerializerMode mode) {
        var parsed = serializeAndParseBack(Lang.TRIX, RDFFormat.TRIX, mode, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    @Test
    void roundTrip_rdfjson_pretty_isIsomorphic() {
        var parsed = serializeAndParseBack(Lang.RDFJSON, RDFFormat.RDFJSON, SerializerMode.PRETTY, sampleTriples());
        assertThat(Models.isomorphic(parsed, sampleTriples()), is(true));
    }

    // ---- quad format round-trips ----

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_nquads_isIsomorphic(SerializerMode mode) {
        var parsed = serializeAndParseBack(Lang.NQUADS, RDFFormat.NQUADS, mode, sampleQuads());
        assertThat(Models.isomorphic(parsed, sampleQuads()), is(true));
    }

    @ParameterizedTest
    @EnumSource(
            value = SerializerMode.class,
            names = {"STREAMING", "PRETTY"})
    void roundTrip_trig_isIsomorphic(SerializerMode mode) {
        var parsed = serializeAndParseBack(Lang.TRIG, RDFFormat.TRIG, mode, sampleQuads());
        assertThat(Models.isomorphic(parsed, sampleQuads()), is(true));
    }

    // ---- namespace handling in pretty mode ----

    @Test
    void prettyTurtle_emitsPrefixDeclarations() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral("hi"));

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(
                    output, Map.of("ex", "http://example.org/", "rdfs", "http://www.w3.org/2000/01/rdf-schema#"));
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString();
        // Directive style forced to "at" via RIOT.symTurtleDirectiveStyle for backward compatibility
        assertThat(result, containsString("@prefix ex:"));
        assertThat(result, containsString("@prefix rdfs:"));
    }
}
