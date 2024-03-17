package io.carml.util;

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
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
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

    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

    private static final IRI DEFAULT_IRI = VALUE_FACTORY.createIRI("http://example.com/default");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        IRI resource = VALUE_FACTORY.createIRI("http://example.com/Fourth");

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
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createIRI("http://example.com/graph");

        // When
        Statement statement = Models.createStatement(
                subjectValue,
                predicateValue,
                objectValue,
                graphValue,
                DEFAULT_GRAPH_MODIFIER,
                VALUE_FACTORY,
                statementConsumer1,
                statementConsumer2);

        // Then
        Statement expected = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createIRI("http://example.com/subject"),
                VALUE_FACTORY.createIRI("http://example.com/predicate"),
                VALUE_FACTORY.createLiteral("object"),
                VALUE_FACTORY.createIRI("http://example.com/graph"));
        assertThat(statement, is(expected));
        verify(statementConsumer1).accept(any());
        verify(statementConsumer2).accept(any());
    }

    @Test
    void givenValuesWithGraphMatchingModifier_whenAllArgsCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createIRI("http://example.com/default");

        // When
        Statement statement = Models.createStatement(
                subjectValue,
                predicateValue,
                objectValue,
                graphValue,
                DEFAULT_GRAPH_MODIFIER,
                VALUE_FACTORY,
                statementConsumer1,
                statementConsumer2);

        // Then
        Statement expected = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createIRI("http://example.com/subject"),
                VALUE_FACTORY.createIRI("http://example.com/predicate"),
                VALUE_FACTORY.createLiteral("object"));
        assertThat(statement, is(expected));
        verify(statementConsumer1).accept(any());
        verify(statementConsumer2).accept(any());
    }

    @Test
    void givenValues_whenCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createIRI("http://example.com/graph");

        // When
        Statement statement = Models.createStatement(subjectValue, predicateValue, objectValue, graphValue);

        // Then
        Statement expected = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createIRI("http://example.com/subject"),
                VALUE_FACTORY.createIRI("http://example.com/predicate"),
                VALUE_FACTORY.createLiteral("object"),
                VALUE_FACTORY.createIRI("http://example.com/graph"));
        assertThat(statement, is(expected));
    }

    @Test
    void givenValuesWithoutGraph_whenCreateStatement_thenReturnStatement() {
        // Given
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");

        // When
        Statement statement = Models.createStatement(subjectValue, predicateValue, objectValue, null);

        // Then
        Statement expected = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createIRI("http://example.com/subject"),
                VALUE_FACTORY.createIRI("http://example.com/predicate"),
                VALUE_FACTORY.createLiteral("object"));
        assertThat(statement, is(expected));
    }

    @Test
    void givenIncorrectSubject_whenCreateStatement_thenThrowException() {
        // Given
        Value subjectValue = VALUE_FACTORY.createLiteral("subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createIRI("http://example.com/graph");

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
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createLiteral("predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createIRI("http://example.com/graph");

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
        Value subjectValue = VALUE_FACTORY.createIRI("http://example.com/subject");
        Value predicateValue = VALUE_FACTORY.createIRI("http://example.com/predicate");
        Value objectValue = VALUE_FACTORY.createLiteral("object");
        Value graphValue = VALUE_FACTORY.createLiteral("graph");

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
        Set<Resource> subjects = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/subject1"),
                VALUE_FACTORY.createIRI("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/predicate1"),
                VALUE_FACTORY.createIRI("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(
                VALUE_FACTORY.createLiteral("object1"),
                VALUE_FACTORY.createLiteral("object2"),
                VALUE_FACTORY.createLiteral("object3"));
        Set<Resource> graphs = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/graph1"),
                VALUE_FACTORY.createIRI("http://example.com/graph2"));

        // When
        Stream<Statement> statementStream = Models.streamCartesianProductStatements(
                subjects,
                predicates,
                objects,
                graphs,
                DEFAULT_GRAPH_MODIFIER,
                VALUE_FACTORY,
                statementConsumer1,
                statementConsumer2);

        // Then
        Model expected = new ModelBuilder()
                .namedGraph(VALUE_FACTORY.createIRI("http://example.com/graph1"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .namedGraph(VALUE_FACTORY.createIRI("http://example.com/graph2"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
        verify(statementConsumer1, times(24)).accept(any());
        verify(statementConsumer2, times(24)).accept(any());
    }

    @Test
    void givenValueSetsWithoutGraphs_whenAllArgsStreamCartesianProductStatements_thenReturnStatementStream() {
        // Given
        Set<Resource> subjects = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/subject1"),
                VALUE_FACTORY.createIRI("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/predicate1"),
                VALUE_FACTORY.createIRI("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(
                VALUE_FACTORY.createLiteral("object1"),
                VALUE_FACTORY.createLiteral("object2"),
                VALUE_FACTORY.createLiteral("object3"));
        Set<Resource> graphs = Set.of();

        // When
        Stream<Statement> statementStream = Models.streamCartesianProductStatements(
                subjects,
                predicates,
                objects,
                graphs,
                DEFAULT_GRAPH_MODIFIER,
                VALUE_FACTORY,
                statementConsumer1,
                statementConsumer2);

        // Then
        Model expected = new ModelBuilder()
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
        verify(statementConsumer1, times(12)).accept(any());
        verify(statementConsumer2, times(12)).accept(any());
    }

    @Test
    void givenValueSets_whenStreamCartesianProductStatements_thenReturnStatementStream() {
        // Given
        Set<Resource> subjects = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/subject1"),
                VALUE_FACTORY.createIRI("http://example.com/subject2"));
        Set<IRI> predicates = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/predicate1"),
                VALUE_FACTORY.createIRI("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(
                VALUE_FACTORY.createLiteral("object1"),
                VALUE_FACTORY.createLiteral("object2"),
                VALUE_FACTORY.createLiteral("object3"));
        Set<Resource> graphs = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/graph1"),
                VALUE_FACTORY.createIRI("http://example.com/graph2"));

        // When
        Stream<Statement> statementStream =
                Models.streamCartesianProductStatements(subjects, predicates, objects, graphs);

        // Then
        Model expected = new ModelBuilder()
                .namedGraph(VALUE_FACTORY.createIRI("http://example.com/graph1"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .namedGraph(VALUE_FACTORY.createIRI("http://example.com/graph2"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .subject(VALUE_FACTORY.createIRI("http://example.com/subject2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate1"), VALUE_FACTORY.createLiteral("object3"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object1"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object2"))
                .add(VALUE_FACTORY.createIRI("http://example.com/predicate2"), VALUE_FACTORY.createLiteral("object3"))
                .build();

        assertThat(statementStream.collect(ModelCollector.toModel()), is(expected));
    }

    @Test
    void givenEmptyTripleValueSet_whenCreateStatement_thenThrowException() {
        // Given
        Set<Resource> subjects = Set.of();
        Set<IRI> predicates = Set.of(
                VALUE_FACTORY.createIRI("http://example.com/predicate1"),
                VALUE_FACTORY.createIRI("http://example.com/predicate2"));
        Set<? extends Value> objects = Set.of(
                VALUE_FACTORY.createLiteral("object1"),
                VALUE_FACTORY.createLiteral("object2"),
                VALUE_FACTORY.createLiteral("object3"));
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
                        VALUE_FACTORY,
                        statementConsumer1,
                        statementConsumer2));

        // Then
        assertThat(
                modelsException.getMessage(),
                is("Could not create cartesian product statements because at least "
                        + "one of subjects, predicates or objects was empty."));
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
