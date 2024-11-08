package io.carml.util.jena;

import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.eclipse.rdf4j.model.util.Values.triple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JenaConvertersTest {

    static Stream<Arguments> toNodeTestArguments() {
        return Stream.of( //
                Arguments.of(
                        iri("http://example.org/resource"),
                        (Predicate<Node>) Node::isURI,
                        Map.of(((Function<Node, String>) Node::toString), "http://example.org/resource")),
                Arguments.of(
                        bnode("123"),
                        (Predicate<Node>) Node::isBlank,
                        Map.of((Function<Node, String>) Node::getBlankNodeLabel, "123")),
                Arguments.of(
                        literal("foo"),
                        (Predicate<Node>) Node::isLiteral,
                        Map.of((Function<Node, String>) Node::getLiteralLexicalForm, "foo")),
                Arguments.of(
                        literal("bar", "en"),
                        (Predicate<Node>) Node::isLiteral,
                        Map.of(
                                (Function<Node, String>) Node::getLiteralLexicalForm,
                                "bar",
                                Node::getLiteralLanguage,
                                "en")),
                Arguments.of(
                        literal("baz", iri("http://example.org/")),
                        (Predicate<Node>) Node::isLiteral,
                        Map.of(
                                (Function<Node, String>) Node::getLiteralLexicalForm,
                                "baz",
                                Node::getLiteralDatatypeURI,
                                "http://example.org/")),
                Arguments.of(
                        triple(iri("http://example.org/resource"), RDFS.LABEL, literal("bar", "nl")),
                        (Predicate<Node>) Node::isNodeTriple,
                        Map.of( //
                                (Function<Node, String>)
                                        node -> node.getTriple().getSubject().getURI(),
                                "http://example.org/resource", //
                                node -> node.getTriple().getPredicate().getURI(),
                                "http://www.w3.org/2000/01/rdf-schema#label", //
                                node -> node.getTriple().getObject().getLiteralLexicalForm(),
                                "bar", //
                                node -> node.getTriple().getObject().getLiteralLanguage(),
                                "nl")));
    }

    @ParameterizedTest
    @MethodSource("toNodeTestArguments")
    void givenValue_whenToNode_thenReturnsExpectedNode(
            Value value, Predicate<Node> nodeTypeChecker, Map<Function<Node, Object>, Object> valueCheckers) {
        // Given
        // When
        var node = JenaConverters.toNode(value);

        // Then
        assertThat(nodeTypeChecker.test(node), is(true));
        valueCheckers.forEach((valueChecker, expectedValue) -> assertThat(valueChecker.apply(node), is(expectedValue)));
    }

    @Test
    void givenUnsupportedValue_whenToNode_thenThrowException() {
        // Given
        var value = new Value() {
            @Override
            public String stringValue() {
                return null;
            }
        };

        // When
        var illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> JenaConverters.toNode(value));

        // Then
        assertThat(illegalArgumentException.getMessage(), startsWith("Unsupported value type"));
    }

    @Test
    void givenStatement_whenToQuad_thenReturnsQuad() {
        // Given
        var statement = Statements.statement(
                iri("http://example.org/resource"), RDFS.LABEL, literal("bar", "nl"), iri("http://example.org/graph"));

        // When
        var quad = JenaConverters.toQuad(statement);

        // Then
        assertThat(quad.getSubject().getURI(), is("http://example.org/resource"));

        assertThat(quad.getPredicate().getURI(), is("http://www.w3.org/2000/01/rdf-schema#label"));

        assertThat(quad.getObject().getLiteralLexicalForm(), is("bar"));

        assertThat(quad.getObject().getLiteralLanguage(), is("nl"));

        assertThat(quad.getGraph().getURI(), is("http://example.org/graph"));
    }
}
