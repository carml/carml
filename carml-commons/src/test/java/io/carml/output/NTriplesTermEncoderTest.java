package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.nio.charset.StandardCharsets;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class NTriplesTermEncoderTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private final NTriplesTermEncoder encoder = NTriplesTermEncoder.withDefaults();

    @Test
    void encodeNTriple_iriSubjectPredicateObject_producesCorrectFormat() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/p");
        var object = VF.createIRI("http://example.org/o");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(result, is("<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"));
    }

    @Test
    void encodeNTriple_bnodeSubject_producesCorrectFormat() {
        var subject = VF.createBNode("node1");
        var predicate = RDF.TYPE;
        var object = VF.createIRI("http://example.org/Thing");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(
                result, is("_:node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Thing> .\n"));
    }

    @Test
    void encodeNTriple_plainLiteral_producesCorrectFormat() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/name");
        var object = VF.createLiteral("Hello World");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(result, is("<http://example.org/s> <http://example.org/name> \"Hello World\" .\n"));
    }

    @Test
    void encodeNTriple_languageTaggedLiteral_producesCorrectFormat() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/label");
        var object = VF.createLiteral("Bonjour", "fr");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(result, is("<http://example.org/s> <http://example.org/label> \"Bonjour\"@fr .\n"));
    }

    @Test
    void encodeNTriple_typedLiteral_producesCorrectFormat() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/age");
        var object = VF.createLiteral("42", XSD.INTEGER);

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://example.org/age>"
                        + " \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"));
    }

    @Test
    void encodeNTriple_xsdStringLiteral_omitsDatatype() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/name");
        var object = VF.createLiteral("Alice", XSD.STRING);

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(result, is("<http://example.org/s> <http://example.org/name> \"Alice\" .\n"));
        assertThat(result, not(containsString("^^")));
    }

    @Test
    void encodeNTriple_literalWithSpecialChars_escapesCorrectly() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/desc");
        var object = VF.createLiteral("say \"hello\"\nand\t\\done");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://example.org/desc>" + " \"say \\\"hello\\\"\\nand\\t\\\\done\" .\n"));
    }

    @Test
    void encodeNTriple_iriWithUnicode_encodesVerbatim() {
        // IRIs are written verbatim (no unicode escaping) per the NT spec for IRIs
        var subject = VF.createIRI("http://example.org/résumé");
        var predicate = RDF.TYPE;
        var object = VF.createIRI("http://example.org/Thing");

        var result = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(result, containsString("<http://example.org/résumé>"));
    }

    @Test
    void encodeNQuad_withGraph_includesGraphField() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/p");
        var object = VF.createIRI("http://example.org/o");
        var graph = VF.createIRI("http://example.org/g");

        var result = new String(encoder.encodeNQuad(subject, predicate, object, graph), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://example.org/p>"
                        + " <http://example.org/o> <http://example.org/g> .\n"));
    }

    @Test
    void encodeNQuad_nullGraph_sameAsNTriple() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/p");
        var object = VF.createIRI("http://example.org/o");

        var nquad = new String(encoder.encodeNQuad(subject, predicate, object, null), StandardCharsets.UTF_8);
        var ntriple = new String(encoder.encodeNTriple(subject, predicate, object), StandardCharsets.UTF_8);

        assertThat(nquad, is(ntriple));
    }

    @Test
    void encodeNQuad_bnodeGraph_producesCorrectFormat() {
        var subject = VF.createIRI("http://example.org/s");
        var predicate = VF.createIRI("http://example.org/p");
        var object = VF.createIRI("http://example.org/o");
        var graph = VF.createBNode("g1");

        var result = new String(encoder.encodeNQuad(subject, predicate, object, graph), StandardCharsets.UTF_8);

        assertThat(result, is("<http://example.org/s> <http://example.org/p> <http://example.org/o> _:g1 .\n"));
    }

    @Test
    void builder_zeroCacheSize_throws() {
        try {
            NTriplesTermEncoder.builder().cacheMaxSize(0);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Cache max size must be positive, got 0"));
        }
    }

    @Test
    void builder_negativeCacheSize_throws() {
        try {
            NTriplesTermEncoder.builder().cacheMaxSize(-5);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Cache max size must be positive, got -5"));
        }
    }

    @Test
    void lruEviction_recomputesCorrectly() {
        // Cache of size 1: only one IRI can be cached at a time. Encoding two different IRIs
        // forces eviction of the first; re-encoding the first IRI must still produce correct output.
        var smallEncoder = NTriplesTermEncoder.builder().cacheMaxSize(1).build();

        var subject = VF.createIRI("http://example.org/s");
        var pred1 = VF.createIRI("http://example.org/pred1");
        var pred2 = VF.createIRI("http://example.org/pred2");
        var object = VF.createLiteral("value");

        var result1 = new String(smallEncoder.encodeNTriple(subject, pred1, object), StandardCharsets.UTF_8);
        // pred1 is now cached, but encoding with pred2 will evict it
        var result2 = new String(smallEncoder.encodeNTriple(subject, pred2, object), StandardCharsets.UTF_8);
        // Re-encode with pred1 after eviction
        var result3 = new String(smallEncoder.encodeNTriple(subject, pred1, object), StandardCharsets.UTF_8);

        assertThat(result1, containsString("<http://example.org/pred1>"));
        assertThat(result2, containsString("<http://example.org/pred2>"));
        assertThat(result3, is(result1));
    }
}
