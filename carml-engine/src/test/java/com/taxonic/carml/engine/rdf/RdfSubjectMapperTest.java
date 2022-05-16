package com.taxonic.carml.engine.rdf;

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

import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.engine.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfSubjectMapperTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private TriplesMap triplesMap;

  @Mock
  private SubjectMap subjectMap;

  @Mock
  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

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
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    // When
    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // Then
    assertThat(rdfSubjectMapper, is(not(nullValue())));
  }

  @Test
  void givenExceptionThrowingSubjectGenerator_whenOfCalled_thenRethrowException() {
    // Given
    when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
    when(subjectMap.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("subject"));
    RdfMappingContext rdfMappingContext = mock(RdfMappingContext.class);

    // When
    Throwable exception = assertThrows(TriplesMapperException.class,
        () -> RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext));

    // Then
    assertThat(exception.getMessage(),
        startsWith("Exception occurred while creating subject generator for blank node resource _:subject in:"));
  }

  @Test
  void givenSingleSubjectGenerator_whenMap_thenReturnGeneratedSubject() {
    // Given
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // When
    RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any());

    // Then
    assertThat(rdfSubjectMapperResult.getSubjects(), hasSize(1));
    assertThat(rdfSubjectMapperResult.getSubjects(), contains(VALUE_FACTORY.createIRI("http://foo.bar/subject")));
  }

  @Test
  void givenMultiSubjectGenerator_whenMap_thenReturnGeneratedSubjects() {
    // Given
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject, subject2));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // When
    RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any());

    // Then
    assertThat(rdfSubjectMapperResult.getSubjects(), hasSize(2));
    assertThat(rdfSubjectMapperResult.getSubjects(), hasItems(VALUE_FACTORY.createIRI("http://foo.bar/subject"),
        VALUE_FACTORY.createIRI("http://foo.bar/subject2")));
  }

  @Test
  void givenClasses_whenMap_thenReturnTypeStatements() {
    // Given
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    IRI class1 = VALUE_FACTORY.createIRI("http://foo.bar/class1");
    IRI class2 = VALUE_FACTORY.createIRI("http://foo.bar/class2");
    when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // When
    RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any());

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2))
        .contains(statement);
    StepVerifier.create(rdfSubjectMapperResult.getTypeStatements())
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .verifyComplete();
  }

  @Test
  void givenClassesAndGraphs_whenMap_thenReturnTypeStatementsPerGraph() {
    // Given
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    IRI class1 = VALUE_FACTORY.createIRI("http://foo.bar/class1");
    IRI class2 = VALUE_FACTORY.createIRI("http://foo.bar/class2");
    when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

    when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1, graphMap2));
    when(rdfTermGeneratorFactory.getGraphGenerator(any())).thenReturn(graphGenerator1)
        .thenReturn(graphGenerator2);
    IRI graph11 = VALUE_FACTORY.createIRI("http://foo.bar/graph11");
    when(graphGenerator1.apply(any())).thenReturn(List.of(graph11));
    IRI graph21 = VALUE_FACTORY.createIRI("http://foo.bar/graph21");
    IRI graph22 = VALUE_FACTORY.createIRI("http://foo.bar/graph22");
    when(graphGenerator2.apply(any())).thenReturn(List.of(graph21, graph22));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // When
    RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any());

    // Then
    assertThat(rdfSubjectMapperResult.getGraphs(), hasItems(graph11, graph21, graph22));

    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1, graph11),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2, graph11),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1, graph21),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2, graph21),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1, graph22),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2, graph22))
        .contains(statement);
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
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    IRI class1 = VALUE_FACTORY.createIRI("http://foo.bar/class1");
    IRI class2 = VALUE_FACTORY.createIRI("http://foo.bar/class2");
    when(subjectMap.getClasses()).thenReturn(Set.of(class1, class2));

    when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1, graphMap2));
    when(rdfTermGeneratorFactory.getGraphGenerator(any())).thenReturn(graphGenerator1)
        .thenReturn(graphGenerator2);
    IRI graph11 = VALUE_FACTORY.createIRI("http://foo.bar/graph11");
    when(graphGenerator1.apply(any())).thenReturn(List.of(graph11));
    IRI graph21 = VALUE_FACTORY.createIRI("http://foo.bar/graph21");
    when(graphGenerator2.apply(any())).thenReturn(List.of(graph21, Rdf.Rr.defaultGraph));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfSubjectMapper rdfSubjectMapper = RdfSubjectMapper.of(subjectMap, triplesMap, rdfMappingContext);

    // When
    RdfSubjectMapper.Result rdfSubjectMapperResult = rdfSubjectMapper.map(any());

    // Then
    assertThat(rdfSubjectMapperResult.getGraphs(), hasItems(graph11, graph21, Rdf.Rr.defaultGraph));

    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1, graph11),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2, graph11),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1, graph21),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2, graph21),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1),
            VALUE_FACTORY.createStatement(subject, RDF.TYPE, class2))
        .contains(statement);
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
