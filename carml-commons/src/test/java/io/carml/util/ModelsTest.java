package io.carml.util;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.getValueFactory;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import java.io.StringWriter;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ModelsTest {

    private static final IRI DEFAULT_IRI = iri("http://example.com/default");

    private static final UnaryOperator<Resource> DEFAULT_GRAPH_MODIFIER =
            graph -> graph.equals(DEFAULT_IRI) ? null : graph;

    @Mock
    private Consumer<Statement> statementConsumer1;

    @Mock
    private Consumer<Statement> statementConsumer2;

    @Test
    void givenDeepModel_whenDescribeResource_thenReturnDeepDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeDeep.ttl")), RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.describeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("describedDeep.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenShallowModel_whenDescribeResource_thenReturnShallowDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeShallow.ttl")),
                RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.describeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("describedShallow.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenDeepModel_whenReverseDescribeResource_thenReturnDeepReverseDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeDeep.ttl")), RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.reverseDescribeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("reverseDescribedDeep.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenShallowModel_whenReverseDescribeResource_thenReturnShallowReverseDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeShallow.ttl")),
                RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.reverseDescribeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("reverseDescribedShallow.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenDeepModel_whenSymmetricDescribeResource_thenReturnDeepSymmetricDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeDeep.ttl")), RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.symmetricDescribeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("symmetricDescribedDeep.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenShallowModel_whenSymmetricDescribeResource_thenReturnShallowSymmetricDescription() {
        // Given
        Model model = Models.parse(
                Objects.requireNonNull(ModelsTest.class.getResourceAsStream("toDescribeShallow.ttl")),
                RDFFormat.TURTLE);
        IRI resource = iri("http://example.com/Fourth");

        // When
        Model described = Models.symmetricDescribeResource(model, resource);

        // Then
        assertThat(
                described,
                isIsomorphicWith(Models.parse(
                        Objects.requireNonNull(ModelsTest.class.getResourceAsStream("symmetricDescribedShallow.ttl")),
                        RDFFormat.TURTLE)));
    }

    @Test
    void givenValues_whenAllArgsCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");
        Value graphValue = iri("http://example.com/graph");

        // When
        Statement statement = Models.createStatement(
                subjectValue,
                predicateValue,
                objectValue,
                graphValue,
                DEFAULT_GRAPH_MODIFIER,
                getValueFactory(),
                statementConsumer1,
                statementConsumer2);

        // Then
        Statement expected = statement(
                iri("http://example.com/subject"),
                iri("http://example.com/predicate"),
                literal("object"),
                iri("http://example.com/graph"));
        assertThat(statement, is(expected));
        verify(statementConsumer1).accept(any());
        verify(statementConsumer2).accept(any());
    }

    @Test
    void givenValuesWithGraphMatchingModifier_whenAllArgsCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");
        Value graphValue = iri("http://example.com/default");

        // When
        Statement statement = Models.createStatement(
                subjectValue,
                predicateValue,
                objectValue,
                graphValue,
                DEFAULT_GRAPH_MODIFIER,
                getValueFactory(),
                statementConsumer1,
                statementConsumer2);

        // Then
        Statement expected = statement(
                iri("http://example.com/subject"), iri("http://example.com/predicate"), literal("object"), null);
        assertThat(statement, is(expected));
        verify(statementConsumer1).accept(any());
        verify(statementConsumer2).accept(any());
    }

    @Test
    void givenValues_whenCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");
        Value graphValue = iri("http://example.com/graph");

        // When
        Statement statement = Models.createStatement(subjectValue, predicateValue, objectValue, graphValue);

        // Then
        Statement expected = statement(
                iri("http://example.com/subject"),
                iri("http://example.com/predicate"),
                literal("object"),
                iri("http://example.com/graph"));
        assertThat(statement, is(expected));
    }

    @Test
    void givenValuesWithoutGraph_whenCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");

        // When
        Statement statement = Models.createStatement(subjectValue, predicateValue, objectValue, null);

        // Then
        Statement expected = statement(
                iri("http://example.com/subject"), iri("http://example.com/predicate"), literal("object"), null);
        assertThat(statement, is(expected));
    }

    @Test
    void givenIncorrectSubject_whenCreateStatement_thenThrowException() {
        // Given
        Value subjectValue = literal("subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");
        Value graphValue = iri("http://example.com/graph");

        // When
        Throwable modelsException = assertThrows(
                ModelsException.class,
                () -> Models.createStatement(subjectValue, predicateValue, objectValue, graphValue));

        // Then
        assertThat(
                modelsException.getMessage(),
                is("Expected subjectValue `\"subject\"` to be instance of Resource, "
                        + "but was org.eclipse.rdf4j.model.impl.SimpleLiteral"));
    }

    @Test
    void givenIncorrectPredicate_whenCreateStatement_thenThrowException() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = literal("predicate");
        Value objectValue = literal("object");
        Value graphValue = iri("http://example.com/graph");

        // When
        Throwable modelsException = assertThrows(
                ModelsException.class,
                () -> Models.createStatement(subjectValue, predicateValue, objectValue, graphValue));

        // Then
        assertThat(
                modelsException.getMessage(),
                is("Expected predicateValue `\"predicate\"` to be instance of IRI, "
                        + "but was org.eclipse.rdf4j.model.impl.SimpleLiteral"));
    }

    @Test
    void givenIncorrectGraph_whenCreateStatement_thenThrowException() {
        // Given
        Value subjectValue = iri("http://example.com/subject");
        Value predicateValue = iri("http://example.com/predicate");
        Value objectValue = literal("object");
        Value graphValue = literal("graph");

        // When
        Throwable modelsException = assertThrows(
                ModelsException.class,
                () -> Models.createStatement(subjectValue, predicateValue, objectValue, graphValue));

        // Then
        assertThat(
                modelsException.getMessage(),
                is("Expected graphValue `\"graph\"` to be instance of Resource, but "
                        + "was org.eclipse.rdf4j.model.impl.SimpleLiteral"));
    }

    @Test
    void givenValueSets_whenAllArgsStreamCartesianProductStatements_thenReturnStatementStream() {
        // Given
        Set<Resource> subjects = Set.of(iri("http://example.com/subject1"), iri("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(iri("http://example.com/predicate1"), iri("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(literal("object1"), literal("object2"), literal("object3"));
        Set<Resource> graphs = Set.of(iri("http://example.com/graph1"), iri("http://example.com/graph2"));

        // When
        Stream<Statement> statementStream = Models.streamCartesianProductStatements(
                subjects,
                predicates,
                objects,
                graphs,
                DEFAULT_GRAPH_MODIFIER,
                getValueFactory(),
                statementConsumer1,
                statementConsumer2);

        // Then
        Model expected = new ModelBuilder()
                .namedGraph(iri("http://example.com/graph1"))
                .subject(iri("http://example.com/subject1"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .subject(iri("http://example.com/subject2"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .namedGraph(iri("http://example.com/graph2"))
                .subject(iri("http://example.com/subject1"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .subject(iri("http://example.com/subject2"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
        verify(statementConsumer1, times(24)).accept(any());
        verify(statementConsumer2, times(24)).accept(any());
    }

    @Test
    void givenValueSetsWithoutGraphs_whenAllArgsStreamCartesianProductStatements_thenReturnStatementStream() {
        // Given
        Set<Resource> subjects = Set.of(iri("http://example.com/subject1"), iri("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(iri("http://example.com/predicate1"), iri("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(literal("object1"), literal("object2"), literal("object3"));
        Set<Resource> graphs = Set.of();

        // When
        Stream<Statement> statementStream = Models.streamCartesianProductStatements(
                subjects,
                predicates,
                objects,
                graphs,
                DEFAULT_GRAPH_MODIFIER,
                getValueFactory(),
                statementConsumer1,
                statementConsumer2);

        // Then
        Model expected = new ModelBuilder()
                .subject(iri("http://example.com/subject1"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .subject(iri("http://example.com/subject2"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
        verify(statementConsumer1, times(12)).accept(any());
        verify(statementConsumer2, times(12)).accept(any());
    }

    @Test
    void givenValueSets_whenStreamCartesianProductStatements_thenReturnStatementStream() {
        // Given
        Set<Resource> subjects = Set.of(iri("http://example.com/subject1"), iri("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(iri("http://example.com/predicate1"), iri("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(literal("object1"), literal("object2"), literal("object3"));
        Set<Resource> graphs = Set.of(iri("http://example.com/graph1"), iri("http://example.com/graph2"));

        // When
        Stream<Statement> statementStream =
                Models.streamCartesianProductStatements(subjects, predicates, objects, graphs);

        // Then
        Model expected = new ModelBuilder()
                .namedGraph(iri("http://example.com/graph1"))
                .subject(iri("http://example.com/subject1"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .subject(iri("http://example.com/subject2"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .namedGraph(iri("http://example.com/graph2"))
                .subject(iri("http://example.com/subject1"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .subject(iri("http://example.com/subject2"))
                .add(iri("http://example.com/predicate1"), literal("object1"))
                .add(iri("http://example.com/predicate1"), literal("object2"))
                .add(iri("http://example.com/predicate1"), literal("object3"))
                .add(iri("http://example.com/predicate2"), literal("object1"))
                .add(iri("http://example.com/predicate2"), literal("object2"))
                .add(iri("http://example.com/predicate2"), literal("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
    }

    @Test
    void givenEmptyTripleValueSet_whenCreateStatement_thenThrowException() {
        // Given
        Set<Resource> subjects = Set.of();
        Set<IRI> predicates = Set.of(iri("http://example.com/predicate1"), iri("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(literal("object1"), literal("object2"), literal("object3"));
        Set<Resource> graphs = Set.of();

        // When
        Throwable modelsException = assertThrows(
                ModelsException.class,
                () -> Models.streamCartesianProductStatements(
                        subjects,
                        predicates,
                        objects,
                        graphs,
                        DEFAULT_GRAPH_MODIFIER,
                        getValueFactory(),
                        statementConsumer1,
                        statementConsumer2));

        // Then
        assertThat(
                modelsException.getMessage(),
                is("Could not create cartesian product statements because at least "
                        + "one of subjects, predicates or objects was empty."));
    }

    @Test
    void remapBlanksForGraph_remapsAllBlankNodes_andPlacesInGraph() {
        // Given
        var vf = getValueFactory();
        var bnode1 = vf.createBNode("b1");
        var bnode2 = vf.createBNode("b2");
        var pred = iri("http://example.com/pred");
        var graph = iri("http://example.com/graph1");

        var model = Stream.of(
                        vf.createStatement(
                                bnode1, iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#first"), literal("1")),
                        vf.createStatement(bnode1, iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest"), bnode2),
                        vf.createStatement(
                                bnode2, iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#first"), literal("2")),
                        vf.createStatement(
                                bnode2,
                                iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest"),
                                iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")))
                .collect(ModelCollector.toModel());

        // When
        var result = Models.remapBlanksForGraph(model, graph, vf);
        var bnodeMap = result.getKey();
        var remappedModel = result.getValue();

        // Then
        // All blank nodes should be remapped
        assertThat(bnodeMap.size(), is(2));
        assertThat(bnodeMap.containsKey(bnode1), is(true));
        assertThat(bnodeMap.containsKey(bnode2), is(true));

        // Remapped blank nodes should be different from originals
        assertThat(bnodeMap.get(bnode1).equals(bnode1), is(false));
        assertThat(bnodeMap.get(bnode2).equals(bnode2), is(false));

        // All statements should be in the target graph
        assertThat(remappedModel.size(), is(4));
        assertThat(remappedModel.stream().allMatch(stmt -> graph.equals(stmt.getContext())), is(true));

        // No original blank nodes should appear
        assertThat(
                remappedModel.stream()
                        .noneMatch(stmt -> stmt.getSubject().equals(bnode1)
                                || stmt.getSubject().equals(bnode2)
                                || stmt.getObject().equals(bnode1)
                                || stmt.getObject().equals(bnode2)),
                is(true));
    }

    @Test
    void remapBlanksForGraph_preservesNonBlankNodes() {
        // Given
        var vf = getValueFactory();
        var subject = iri("http://example.com/subject");
        var bnode = vf.createBNode("b1");
        var graph = iri("http://example.com/graph1");

        var model = Stream.of(
                        vf.createStatement(subject, iri("http://example.com/pred"), bnode),
                        vf.createStatement(bnode, iri("http://example.com/val"), literal("v")))
                .collect(ModelCollector.toModel());

        // When
        var result = Models.remapBlanksForGraph(model, graph, vf);
        var remappedModel = result.getValue();

        // Then
        // IRI subject should be preserved
        assertThat(remappedModel.stream().anyMatch(stmt -> stmt.getSubject().equals(subject)), is(true));
        // Literal object should be preserved
        assertThat(remappedModel.stream().anyMatch(stmt -> stmt.getObject().equals(literal("v"))), is(true));
    }

    static Matcher<Model> isIsomorphicWith(final Model expected) {
        return new TypeSafeMatcher<>() {

            @Override
            protected boolean matchesSafely(Model actual) {
                return org.eclipse.rdf4j.model.util.Models.isomorphic(actual, expected);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(String.format("Model with %s statements.", expected.size()));
            }

            @Override
            protected void describeMismatchSafely(final Model item, final Description mismatchDescription) {
                mismatchDescription.appendText(String.format("Model with %s statements.%n%n", item.size()));

                Sets.SetView<Statement> statementsMissing = Sets.difference(expected, item);
                mismatchDescription.appendText(String.format("Statements expected but missing:%n%n"));
                mismatchDescription.appendText(modelToString(new LinkedHashModel(statementsMissing)));

                Sets.SetView<Statement> surplusStatements = Sets.difference(item, expected);
                mismatchDescription.appendText(String.format("Statements that were not expected:%n%n"));
                mismatchDescription.appendText(modelToString(new LinkedHashModel(surplusStatements)));
            }

            private String modelToString(final Model model) {
                model.setNamespace("ex", "http://example.org/");

                StringWriter stringWriter = new StringWriter();
                Rio.write(model, stringWriter, RDFFormat.TURTLE);
                return stringWriter
                        .toString()
                        .replace("\r\n", System.lineSeparator())
                        .replace("\r", System.lineSeparator());
            }
        };
    }
}
