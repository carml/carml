package io.carml.engine.rdf.cc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactoryException;
import io.carml.engine.rdf.RdfMappedValue;
import io.carml.engine.rdf.RdfTermGeneratorFactory;
import io.carml.model.BaseObjectMap;
import io.carml.model.GatherMap;
import io.carml.model.Join;
import io.carml.model.ObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf.Rml;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class RdfListOrContainerGeneratorTest {

    private final SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

    @Mock
    private GatherMap gatherMap;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private SubjectMap subjectMap;

    @Mock
    private TermGenerator<Resource> subjectGenerator;

    @Mock
    private ObjectMap objectMap;

    @Mock
    private TermGenerator<Value> objectGenerator;

    @BeforeEach
    void setup() {
        lenient().when(gatherMap.asSubjectMap()).thenReturn(subjectMap);
        lenient().when(gatherMap.getStrategy()).thenReturn(Rml.append);
        lenient().when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    }

    @Test
    void handleEmpty_gatherAsList_returnsRdfNil() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getLogicalTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getValue(), is(RDF.NIL));
    }

    @Test
    void handleEmpty_gatherAsBag_returnsEmptyTypedContainer() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.BAG);
        when(gatherMap.getLogicalTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.BAG));
        var statements = Flux.from(container.getResults()).collectList().block();
        assertNotNull(statements);
        assertThat(
                statements.stream()
                        .anyMatch(s -> s.getSubject().equals(container.getContainer())
                                && s.getPredicate().equals(RDF.TYPE)
                                && s.getObject().equals(RDF.BAG)),
                is(true));
        // Empty container should have only the type triple
        assertThat(statements, hasSize(1));
    }

    @Test
    void handleEmpty_gatherAsSeq_returnsEmptyTypedContainer() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.SEQ);
        when(gatherMap.getLogicalTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.SEQ));
        var statements = Flux.from(container.getResults()).collectList().block();
        assertNotNull(statements);
        assertThat(
                statements.stream()
                        .anyMatch(s -> s.getSubject().equals(container.getContainer())
                                && s.getPredicate().equals(RDF.TYPE)
                                && s.getObject().equals(RDF.SEQ)),
                is(true));
        assertThat(statements, hasSize(1));
    }

    @Test
    void handleEmpty_gatherAsAlt_returnsEmptyTypedContainer() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.ALT);
        when(gatherMap.getLogicalTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.ALT));
        var statements = Flux.from(container.getResults()).collectList().block();
        assertNotNull(statements);
        assertThat(
                statements.stream()
                        .anyMatch(s -> s.getSubject().equals(container.getContainer())
                                && s.getPredicate().equals(RDF.TYPE)
                                && s.getObject().equals(RDF.ALT)),
                is(true));
        assertThat(statements, hasSize(1));
    }

    @Test
    void nonAppendStrategy_withTwoGathers_returnsCartesianProductList() {
        // Given
        var objectMap2 = org.mockito.Mockito.mock(ObjectMap.class);
        @SuppressWarnings("unchecked")
        TermGenerator<Value> objectGenerator2 = org.mockito.Mockito.mock(TermGenerator.class);

        when(gatherMap.getStrategy()).thenReturn(Rml.cartesianProduct);
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap, objectMap2));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap2)).thenReturn(objectGenerator2);

        var val1 = RdfMappedValue.<Value>of(valueFactory.createLiteral("a"));
        var val2 = RdfMappedValue.<Value>of(valueFactory.createLiteral("b"));
        var val3 = RdfMappedValue.<Value>of(valueFactory.createLiteral("x"));

        when(objectGenerator.apply(any(), any())).thenReturn(List.of(val1, val2));
        when(objectGenerator2.apply(any(), any())).thenReturn(List.of(val3));

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        when(gatherMap.getGatherAs()).thenReturn(RDF.SEQ);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then — cartesian product of [a, b] x [x] = 2 containers, each with 2 elements
        assertThat(result, hasSize(2));
        for (var mappedValue : result) {
            assertThat(mappedValue, instanceOf(RdfContainer.class));
        }
    }

    @Test
    void nonAppendStrategy_withDuplicateValues_preservesDuplicates() {
        // Given
        when(gatherMap.getStrategy()).thenReturn(Rml.cartesianProduct);
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);

        var val = RdfMappedValue.<Value>of(valueFactory.createLiteral("dup"));
        when(objectGenerator.apply(any(), any())).thenReturn(List.of(val, val));

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then — two duplicate values produce two separate lists (one element each)
        assertThat(result, hasSize(2));
        for (var mappedValue : result) {
            assertThat(mappedValue, instanceOf(RdfList.class));
        }
    }

    @Test
    void apply_givenJoinlessRefObjectMapGather_returnsParentSubjectValues() {
        // Given
        var parentTriplesMap = mock(TriplesMap.class);
        var parentSubjectMap = mock(SubjectMap.class);
        when(parentTriplesMap.getSubjectMaps()).thenReturn(Set.of(parentSubjectMap));

        @SuppressWarnings("unchecked")
        TermGenerator<Resource> parentSubjectGenerator = mock(TermGenerator.class);
        var parentIri = RdfMappedValue.<Resource>of(valueFactory.createIRI("http://example.com/Parent/1"));
        when(parentSubjectGenerator.apply(any(), any())).thenReturn(List.of(parentIri));
        when(rdfTermGeneratorFactory.getSubjectGenerator(parentSubjectMap)).thenReturn(parentSubjectGenerator);

        var refObjectMap = mock(RefObjectMap.class);
        when(refObjectMap.getJoinConditions()).thenReturn(Set.of());
        when(refObjectMap.getParentTriplesMap()).thenReturn(parentTriplesMap);

        when(gatherMap.getGathers()).thenReturn(List.of(refObjectMap));
        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        assertThat(result.get(0), instanceOf(RdfList.class));
    }

    @Test
    void apply_givenJoinedRefObjectMapGather_withNoPrefixMap_throwsTermGeneratorFactoryException() {
        // Given — joined ROM in gather but no refObjectMapPrefixes configured (misconfiguration)
        var join = mock(Join.class);
        var refObjectMap = mock(RefObjectMap.class);
        when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join));

        when(gatherMap.getGathers()).thenReturn(List.of(refObjectMap));

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When / Then
        assertThrows(TermGeneratorFactoryException.class, () -> generator.apply(null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void apply_givenJoinedRefObjectMapGather_withPrefixMap_returnsParentSubjects() {
        // Given — joined ROM in gather with refObjectMapPrefixes configured
        var join = mock(Join.class);
        var refObjectMap = mock(RefObjectMap.class);
        when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join));

        var parentTriplesMap = mock(TriplesMap.class);
        var parentSubjectMap = mock(SubjectMap.class);
        when(refObjectMap.getParentTriplesMap()).thenReturn(parentTriplesMap);
        when(parentTriplesMap.getSubjectMaps()).thenReturn(Set.of(parentSubjectMap));
        when(parentSubjectMap.getExpressionMapExpressionSet()).thenReturn(Set.of("ID"));
        when(parentSubjectMap.applyExpressionAdapter(any())).thenReturn(parentSubjectMap);

        // Subject generator returns one IRI per position evaluation
        var parentSubjectGenerator = mock(TermGenerator.class);
        when(rdfTermGeneratorFactory.getSubjectGenerator(parentSubjectMap)).thenReturn(parentSubjectGenerator);
        var subject1 = RdfMappedValue.<Resource>of(valueFactory.createIRI("http://example.com/Student/10"));
        var subject2 = RdfMappedValue.<Resource>of(valueFactory.createIRI("http://example.com/Student/20"));
        when(parentSubjectGenerator.apply(any(), any()))
                .thenReturn(List.of(subject1))
                .thenReturn(List.of(subject2));

        when(gatherMap.getGathers()).thenReturn(List.of(refObjectMap));
        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        var prefixes = java.util.Map.of(refObjectMap, "_ref0.");

        // Expression evaluation returns aggregated list values for prefixed fields
        io.carml.logicalsourceresolver.ExpressionEvaluation exprEval = expr -> {
            if ("_ref0.ID".equals(expr)) {
                return java.util.Optional.of(List.of("10", "20"));
            }
            return java.util.Optional.empty();
        };

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory, prefixes);

        // When
        var result = generator.apply(exprEval, null);

        // Then — should produce one RDF list with 2 parent subjects
        assertThat(result, hasSize(1));
        assertThat(result.get(0), instanceOf(RdfList.class));
    }

    @Test
    void apply_givenUnsupportedBaseObjectMapType_throwsTermGeneratorFactoryException() {
        // Given
        var unknownObjectMap = mock(BaseObjectMap.class);
        when(gatherMap.getGathers()).thenReturn(List.of(unknownObjectMap));

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When / Then
        assertThrows(TermGeneratorFactoryException.class, () -> generator.apply(null, null));
    }

    @Test
    void handleEmpty_allowEmptyFalse_returnsEmptyList() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(false);

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyAsStream_cartesianProduct_isLazy() {
        // Given — two 1000-element gather slots producing 1_000_000 cartesian-product tuples.
        // We consume only 10 of them and assert the head generator was invoked exactly 10 times,
        // proving the stream is lazy (no eager materialization of the full product).
        var objectMap2 = mock(ObjectMap.class);
        TermGenerator<Value> objectGenerator2 = mock(TermGenerator.class);

        when(gatherMap.getStrategy()).thenReturn(Rml.cartesianProduct);
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap, objectMap2));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap2)).thenReturn(objectGenerator2);

        var slot1 = java.util.stream.IntStream.range(0, 1_000)
                .mapToObj(i -> RdfMappedValue.<Value>of(valueFactory.createLiteral("a" + i)))
                .toList();
        var slot2 = java.util.stream.IntStream.range(0, 1_000)
                .mapToObj(i -> RdfMappedValue.<Value>of(valueFactory.createLiteral("b" + i)))
                .toList();

        when(objectGenerator.apply(any(), any())).thenReturn(slot1);
        when(objectGenerator2.apply(any(), any())).thenReturn(slot2);

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When — limit to 10 emissions
        try (var stream = generator.applyAsStream(null, null)) {
            var first10 = stream.limit(10).toList();
            assertThat(first10, hasSize(10));
        }

        // Then — head generator must have been called once per emitted tuple
        verify(subjectGenerator, times(10)).apply(any(), any());
        // Sanity: gather generators are evaluated once up-front (per applyAsStream call), not per tuple
        verify(objectGenerator, atLeast(1)).apply(any(), any());
        verify(objectGenerator2, atLeast(1)).apply(any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyAsStream_andApply_produceEqualResults() {
        // Given — small 3x3 cartesian product
        var objectMap2 = mock(ObjectMap.class);
        TermGenerator<Value> objectGenerator2 = mock(TermGenerator.class);

        when(gatherMap.getStrategy()).thenReturn(Rml.cartesianProduct);
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap, objectMap2));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap2)).thenReturn(objectGenerator2);

        var slot1 = List.of(
                RdfMappedValue.<Value>of(valueFactory.createLiteral("a")),
                RdfMappedValue.<Value>of(valueFactory.createLiteral("b")),
                RdfMappedValue.<Value>of(valueFactory.createLiteral("c")));
        var slot2 = List.of(
                RdfMappedValue.<Value>of(valueFactory.createLiteral("1")),
                RdfMappedValue.<Value>of(valueFactory.createLiteral("2")),
                RdfMappedValue.<Value>of(valueFactory.createLiteral("3")));

        when(objectGenerator.apply(any(), any())).thenReturn(slot1);
        when(objectGenerator2.apply(any(), any())).thenReturn(slot2);

        var headValue = RdfMappedValue.<Resource>of(valueFactory.createBNode("head1"));
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(headValue));

        when(gatherMap.getGatherAs()).thenReturn(RDF.LIST);
        when(gatherMap.getExpressionMapExpressionSet()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var fromApply = generator.apply(null, null);
        List<? extends Object> fromStream;
        try (var stream = generator.applyAsStream(null, null)) {
            fromStream = stream.toList();
        }

        // Then — both produce 9 RdfList instances with equal element values per position
        assertThat(fromApply, hasSize(9));
        assertThat(fromStream, hasSize(9));
        for (int i = 0; i < fromApply.size(); i++) {
            var a = (RdfList<Value>) fromApply.get(i);
            var b = (RdfList<Value>) fromStream.get(i);
            assertThat(a.getElements(), is(b.getElements()));
        }
    }
}
