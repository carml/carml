package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.model.GraphMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf.Rml;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfSubjectMapperTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private SubjectMap subjectMap;

    @Mock
    private ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStoreProvider;

    @Mock
    private TermGenerator<Resource> subjectGenerator;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private GraphMap graphMap1;

    @Mock
    private GraphMap graphMap2;

    @Mock
    private TermGenerator<Resource> graphGenerator1;

    @Mock
    private TermGenerator<Resource> graphGenerator2;

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfSubjectMapper() {
        // Given
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // Then
        assertThat(rdfSubjectMapper, is(not(nullValue())));
    }

    @Test
    void givenExceptionThrowingSubjectGenerator_whenOfCalled_thenRethrowException() {
        // Given
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(subjectMap.getAsResource()).thenReturn(bnode("subject"));
        RdfMapperConfig rdfMappingConfig = mock(RdfMapperConfig.class);

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class, () -> RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith(
                        "Exception occurred while creating subject generator for blank node resource _:subject in:"));
    }

    @Test
    void givenAllParams_whenOfJoiningCalled_thenConstructRdfSubjectMapper() {
        // Given
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.ofJoining(subjectMap, triplesMap, rdfMappingConfig);

        // Then
        assertThat(rdfSubjectMapper, is(not(nullValue())));
    }

    @Test
    void givenExceptionThrowingSubjectGenerator_whenOfJoiningCalled_thenRethrowException() {
        // Given
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(subjectMap.getAsResource()).thenReturn(bnode("subject"));
        RdfMapperConfig rdfMappingConfig = mock(RdfMapperConfig.class);

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class,
                () -> RdfSubjectMapper.ofJoining(subjectMap, triplesMap, rdfMappingConfig));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith(
                        "Exception occurred while creating subject generator for blank node resource _:subject in:"));
    }

    @Test
    void givenSingleSubjectGenerator_whenMap_thenReturnGeneratedSubject() {
        // Given
        var subject = iri("http://foo.bar/subject");
        MappedValue<Resource> mappedSubject = RdfMappedValue.of(subject);
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(mappedSubject));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // When
        RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any(), any());

        // Then
        assertThat(rdfSubjectMapperResult.getSubjects(), hasSize(1));
        assertThat(rdfSubjectMapperResult.getSubjects(), contains(mappedSubject));
    }

    @Test
    void givenMultiSubjectGenerator_whenMap_thenReturnGeneratedSubjects() {
        // Given
        var subject = iri("http://foo.bar/subject");
        var subject2 = iri("http://foo.bar/subject2");
        when(subjectGenerator.apply(any(), any()))
                .thenReturn(Set.of(RdfMappedValue.of(subject), RdfMappedValue.of(subject2)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // When
        RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any(), any());

        // Then
        assertThat(rdfSubjectMapperResult.getSubjects(), hasSize(2));
        assertThat(
                rdfSubjectMapperResult.getSubjects(),
                hasItems(RdfMappedValue.of(subject), RdfMappedValue.of(subject2)));
    }

    @Test
    void givenClasses_whenMap_thenReturnTypeStatements() {
        // Given
        var subject = iri("http://foo.bar/subject");
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(RdfMappedValue.of(subject)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(RdfMappedValue.of(subject)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        IRI class1 = iri("http://foo.bar/class1");
        IRI class2 = iri("http://foo.bar/class2");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // When
        RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any(), any());

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject, RDF.TYPE, class1, null), statement(subject, RDF.TYPE, class2, null))
                .contains(Mono.from(mappedStatement.getResults()).block());
        StepVerifier.create(rdfSubjectMapperResult.getTypeStatements())
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenClassesAndGraphs_whenMap_thenReturnTypeStatementsPerGraph() {
        // Given
        var subject = iri("http://foo.bar/subject");
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(RdfMappedValue.of(subject)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        var class1 = iri("http://foo.bar/class1");
        var class2 = iri("http://foo.bar/class2");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1, graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(any()))
                .thenReturn(graphGenerator1)
                .thenReturn(graphGenerator2);
        var graph11 = iri("http://foo.bar/graph11");
        MappedValue<Resource> mappedGraph11 = RdfMappedValue.of(graph11);
        when(graphGenerator1.apply(any(), any())).thenReturn(Set.of(mappedGraph11));
        var graph21 = iri("http://foo.bar/graph21");
        MappedValue<Resource> mappedGraph21 = RdfMappedValue.of(graph21);
        var graph22 = iri("http://foo.bar/graph22");
        MappedValue<Resource> mappedGraph22 = RdfMappedValue.of(graph22);
        when(graphGenerator2.apply(any(), any())).thenReturn(Set.of(mappedGraph21, mappedGraph22));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // When
        RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any(), any());

        // Then
        assertThat(rdfSubjectMapperResult.getGraphs(), hasItems(mappedGraph11, mappedGraph21, mappedGraph22));

        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject, RDF.TYPE, class1, graph11),
                        statement(subject, RDF.TYPE, class2, graph11),
                        statement(subject, RDF.TYPE, class1, graph21),
                        statement(subject, RDF.TYPE, class2, graph21),
                        statement(subject, RDF.TYPE, class1, graph22),
                        statement(subject, RDF.TYPE, class2, graph22))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(rdfSubjectMapperResult.getTypeStatements())
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenClassesAndDefaultGraph_whenMap_thenReturnTypeStatementsPerGraph() {
        // Given
        var subject = iri("http://foo.bar/subject");
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(RdfMappedValue.of(subject)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        when(subjectGenerator.apply(any(), any())).thenReturn(Set.of(RdfMappedValue.of(subject)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        IRI class1 = iri("http://foo.bar/class1");
        IRI class2 = iri("http://foo.bar/class2");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1, graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(any()))
                .thenReturn(graphGenerator1)
                .thenReturn(graphGenerator2);
        var graph11 = iri("http://foo.bar/graph11");
        MappedValue<Resource> mappedGraph11 = RdfMappedValue.of(graph11);
        when(graphGenerator1.apply(any(), any())).thenReturn(Set.of(mappedGraph11));
        var graph21 = iri("http://foo.bar/graph21");
        MappedValue<Resource> mappedGraph21 = RdfMappedValue.of(graph21);
        MappedValue<Resource> mappedDefaultGraph = RdfMappedValue.of(Rml.defaultGraph);
        when(graphGenerator2.apply(any(), any())).thenReturn(Set.of(mappedGraph21, mappedDefaultGraph));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingConfig);

        // When
        RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any(), any());

        // Then
        assertThat(rdfSubjectMapperResult.getGraphs(), hasItems(mappedGraph11, mappedGraph21, mappedDefaultGraph));

        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject, RDF.TYPE, class1, graph11),
                        statement(subject, RDF.TYPE, class2, graph11),
                        statement(subject, RDF.TYPE, class1, graph21),
                        statement(subject, RDF.TYPE, class2, graph21),
                        statement(subject, RDF.TYPE, class1, null),
                        statement(subject, RDF.TYPE, class2, null))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(rdfSubjectMapperResult.getTypeStatements())
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }
}
