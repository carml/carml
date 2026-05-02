package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.StreamingTermGenerator;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.rdf.cc.MergeableRdfList;
import io.carml.engine.rdf.cc.RdfContainer;
import io.carml.engine.rdf.cc.RdfList;
import io.carml.model.GraphMap;
import io.carml.model.LogicalSource;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
import io.carml.vocab.Rdf.Rml;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfPredicateObjectMapperTest {

    @Mock
    private PredicateObjectMap pom;

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private LogicalSource logicalSource;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private Set<MappedValue<Resource>> subjects;

    @Mock
    private Set<MappedValue<Resource>> subjectGraphs;

    @Mock
    private TermGenerator<IRI> predicateGenerator1;

    @Mock
    private PredicateMap predicateMap1;

    @Mock
    private TermGenerator<Value> objectGenerator1;

    @Mock
    private ObjectMap objectMap1;

    @Mock
    private TermGenerator<Resource> graphGenerator1;

    @Mock
    private GraphMap graphMap1;

    @Mock
    private RefObjectMap refObjectMap1;

    @Mock
    private TriplesMap triplesMap2;

    @Mock
    private LogicalSource logicalSource2;

    @Mock
    private SubjectMap subjectMap2;

    @Mock
    private TermGenerator<Resource> subjectGenerator2;

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfPredicateObjectMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        // When
        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        // Then
        assertThat(rdfPredicateObjectMapper, is(not(nullValue())));
    }

    @Test
    void givenSingleJoinlessRefObjectMapWithDifferentLogicalSource_whenOfCalled_thenThrowException() {
        // Given
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(logicalSource.getAsResource()).thenReturn(bnode("logicalSource"));

        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);

        when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
        when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);
        when(refObjectMap1.getAsResource()).thenReturn(bnode("refObjectMap1"));

        when(triplesMap2.getLogicalSource()).thenReturn(logicalSource2);
        when(triplesMap2.asRdf()).thenReturn(new ModelBuilder().build());
        when(logicalSource2.getAsResource()).thenReturn(bnode("logicalSource2"));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class, () -> RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig));

        // Then
        assertThat(exception.getMessage(), startsWith("Logical sources are not equal."));
    }

    @Test
    void givenPredicateGeneratorReturningEmpty_whenMap_thenReturnNothing() {
        // Given
        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of());

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        StepVerifier.create(pomStatements).verifyComplete();
    }

    @Test
    void givenSingleValuedSubPredObjGenerators_whenMap_thenGenerateSingleStatement() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Objects.equals(
                statement(subject1, predicate1, object1, null),
                Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenSubjectGraphsAndGraphGenerators_whenMap_generatesStatementsForAllGraphs() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(graph1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1))
                .contains(Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void
            givenSubjectGraphsWithDefaultGraphAndGraphGeneratorsWithDefaultGraph_whenMap_generatesStatementsForAllGraphs() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1), RdfMappedValue.of(Rml.defaultGraph));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator1.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(graph1), RdfMappedValue.of(Rml.defaultGraph)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1),
                        statement(subject1, predicate1, object1, null))
                .contains(Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenSingleJoinlessRefObjectMap_whenMap_thenReturnJoinedStatementDirectly() {
        // Given
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);

        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
        when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);

        when(triplesMap2.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap2.getSubjectMaps()).thenReturn(Set.of(subjectMap2));

        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

        IRI subject2 = iri("http://foo.bar/subject2");
        when(subjectGenerator2.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subject2)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then

        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Objects.equals(
                statement(subject1, predicate1, subject2, subjectGraph1),
                Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenNonMergeableRdfContainer_whenMapToBytes_thenEmitsLinkingAndStructuralTriples() {
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);

        var bagBNode = bnode("bag1");
        var bag = RdfContainer.<Value>builder()
                .type(org.eclipse.rdf4j.model.vocabulary.RDF.BAG)
                .container(bagBNode)
                .elements(new ArrayList<>(List.of(literal("v1"), literal("v2"))))
                .build();
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(bag));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, Set.<MappedValue<Resource>>of());
        var encoder = NTriplesTermEncoder.withDefaults();
        var mergeableAccumulator = new ArrayList<MappingResult<Statement>>();

        // When
        var bytes = rdfPredicateObjectMapper
                .mapToBytes(null, null, subjectsAndSubjectGraphs, encoder, mergeableAccumulator, false)
                .collectList()
                .block();

        // Then — decode and inspect emitted triple lines
        var lines = Objects.requireNonNull(bytes).stream()
                .map(b -> new String(b, StandardCharsets.UTF_8).trim())
                .toList();

        // Linking triple: <subject, predicate, _:bag>
        assertThat(lines, hasItem(containsString("<http://foo.bar/subject1> <http://foo.bar/predicate1> _:bag1")));

        // Structural triple: <_:bag, rdf:type, rdf:Bag>
        assertThat(
                lines,
                hasItem(containsString("_:bag1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag>")));

        // Member triples: <_:bag, rdf:_1, "v1"> and <_:bag, rdf:_2, "v2">
        assertThat(lines, hasItem(containsString("_:bag1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> \"v1\"")));
        assertThat(lines, hasItem(containsString("_:bag1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> \"v2\"")));
    }

    @Test
    void givenStreamingObjectGenerator_whenMapToBytes_thenEmitsLinkingAndStructuralTriples() {
        // Given — wire a StreamingTermGenerator that yields 100 RdfList instances on demand. Each
        // list has a unique head BNode and one literal element. The byte path must emit the link
        // triple <subject, predicate, headN> + the cons-cell statements for every emitted list.
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));

        StreamingTermGenerator<Value> streamingGen = new StreamingTermGenerator<>() {
            @Override
            public java.util.stream.Stream<MappedValue<Value>> applyAsStream(
                    io.carml.logicalsourceresolver.ExpressionEvaluation expressionEvaluation,
                    io.carml.logicalsourceresolver.DatatypeMapper datatypeMapper) {
                return java.util.stream.IntStream.range(0, 100).mapToObj(i -> {
                    var headBNode = bnode("list" + i);
                    return (MappedValue<Value>) RdfList.<Value>builder()
                            .head(headBNode)
                            .elements(new ArrayList<>(List.of(literal("v" + i))))
                            .build();
                });
            }
        };
        // The factory mock must return a StreamingTermGenerator for objectMap1 so the partition in
        // RdfPredicateObjectMapper.mapToBytes routes it through the streaming flux path.
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(streamingGen);

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, Set.<MappedValue<Resource>>of());
        var encoder = NTriplesTermEncoder.withDefaults();
        var mergeableAccumulator = new ArrayList<MappingResult<Statement>>();

        // When
        var bytes = rdfPredicateObjectMapper
                .mapToBytes(null, null, subjectsAndSubjectGraphs, encoder, mergeableAccumulator, false)
                .collectList()
                .block();

        // Then — every list should appear: linking triple + 2 cons cells (rdf:first, rdf:rest)
        var lines = Objects.requireNonNull(bytes).stream()
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .toList();

        for (int i = 0; i < 100; i++) {
            // Linking triple: <subject, predicate, _:listN>
            int idx = i;
            assertThat(
                    lines,
                    hasItem(containsString(
                            "<http://foo.bar/subject1> <http://foo.bar/predicate1> _:list" + idx + " .")));
            // First cons cell: <_:listN, rdf:first, "vN">
            assertThat(
                    lines,
                    hasItem(containsString(
                            "_:list" + idx + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"v" + idx + "\"")));
        }
    }

    @Test
    void givenMergeableGatherMapWithGraphMap_whenMap_thenRoutesToMergeableAccumulator() {
        // Given — an eager TermGenerator that returns a MergeableRdfList combined with a graphMap
        // on the POM. The mergeable-with-graphs path must NOT be routed through the streaming flux
        // (which would trip the IllegalStateException guard); instead it must be wrapped via
        // scopeMergeableForGraphs and emitted as a MergeableRdfList MappingResult.
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);

        var headBNode = bnode("mergeableHead");
        var mergeableList = MergeableRdfList.<Value>builder()
                .head(headBNode)
                .elements(new ArrayList<>(List.of(literal("v1"))))
                .build();
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(mergeableList));

        // POM has a graphMap so the mergeable-with-graphs path is exercised.
        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(graph1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, Set.<MappedValue<Resource>>of());

        // When
        var results = rdfPredicateObjectMapper
                .map(null, null, subjectsAndSubjectGraphs)
                .collectList()
                .block();

        // Then — drain succeeds without IllegalStateException, and the mergeable accumulator path
        // produced a MergeableRdfList MappingResult (the post-graph-scope instance).
        assertThat(Objects.requireNonNull(results), hasItem(instanceOf(MergeableRdfList.class)));
    }

    @Test
    void givenMergeableGatherMapWithGraphMap_whenMapToBytes_thenRoutesToMergeableAccumulator() {
        // Given — same shape as the statement test, exercising the byte path. The mergeable
        // collection is collected into the mergeableAccumulator (which the caller drains later);
        // the returned byte flux must complete without throwing.
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);

        var headBNode = bnode("mergeableHead");
        var mergeableList = MergeableRdfList.<Value>builder()
                .head(headBNode)
                .elements(new ArrayList<>(List.of(literal("v1"))))
                .build();
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(mergeableList));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(graph1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, Set.<MappedValue<Resource>>of());
        var encoder = NTriplesTermEncoder.withDefaults();
        var mergeableAccumulator = new ArrayList<MappingResult<Statement>>();

        // When — drain the byte flux. Mergeables resolve to bytes when the merged tail is emitted
        // by the caller, so the in-flight flux returns no bytes here but must complete without error.
        StepVerifier.create(rdfPredicateObjectMapper.mapToBytes(
                        null, null, subjectsAndSubjectGraphs, encoder, mergeableAccumulator, false))
                .verifyComplete();

        // Then — the mergeable accumulator received the post-graph-scope MergeableRdfList.
        assertThat(mergeableAccumulator, hasItem(instanceOf(MergeableRdfList.class)));
    }

    @Test
    void givenStreamingObjectGenerator_whenMapToBytesAndTake_thenGeneratorIsBoundedByDemand() {
        // Given — a StreamingTermGenerator backed by an AtomicInteger counter that increments
        // every time a value is emitted from the source stream. The source is sized at 1_000_000
        // so an eager-materialization regression (e.g. a downstream toList) is detectable: the
        // counter would jump to 1_000_000 and the upper-bound assertion would fail.
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));

        int sourceSize = 1_000_000;
        var counter = new AtomicInteger(0);
        StreamingTermGenerator<Value> streamingGen = new StreamingTermGenerator<>() {
            @Override
            public java.util.stream.Stream<MappedValue<Value>> applyAsStream(
                    io.carml.logicalsourceresolver.ExpressionEvaluation expressionEvaluation,
                    io.carml.logicalsourceresolver.DatatypeMapper datatypeMapper) {
                return java.util.stream.IntStream.range(0, sourceSize).mapToObj(i -> {
                    counter.incrementAndGet();
                    var headBNode = bnode("list" + i);
                    return (MappedValue<Value>) RdfList.<Value>builder()
                            .head(headBNode)
                            .elements(new ArrayList<>(List.of(literal("v" + i))))
                            .build();
                });
            }
        };
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(streamingGen);

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, Set.<MappedValue<Resource>>of());
        var encoder = NTriplesTermEncoder.withDefaults();
        var mergeableAccumulator = new ArrayList<MappingResult<Statement>>();

        // Take K bytes from the byte flux. Each source RdfList produces ~3 byte outputs (linking
        // triple + first cons cell + rdf:nil terminator), so consuming K=10 bytes pulls only a
        // handful of source values. The upper-bound budget is generous (1024) to absorb Reactor
        // prefetch overhead — Flux.fromStream uses a default prefetch of 256 and concatMap layers
        // on small inner-prefetch defaults — but is still 3 orders of magnitude below sourceSize,
        // so a regression to eager materialization would fail this assertion decisively.
        int take = 10;
        int prefetchUpperBound = 1024;

        // When — take only K bytes and confirm production.
        var taken = rdfPredicateObjectMapper
                .mapToBytes(null, null, subjectsAndSubjectGraphs, encoder, mergeableAccumulator, false)
                .take(take)
                .collectList()
                .block();

        // Then — exactly K bytes were produced (the take wasn't a no-op) and the source counter
        // proves laziness: bounded above by the prefetch budget, well below sourceSize.
        assertThat(Objects.requireNonNull(taken).size(), is(take));
        assertThat(counter.get(), is(both(greaterThanOrEqualTo(1)).and(lessThanOrEqualTo(prefetchUpperBound))));
    }
}
