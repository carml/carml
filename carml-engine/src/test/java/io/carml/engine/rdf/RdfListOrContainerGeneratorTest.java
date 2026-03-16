package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactoryException;
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
        when(gatherMap.getTargets()).thenReturn(Set.of());

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
        when(gatherMap.getTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.BAG));
        assertThat(container.getModel().contains((Resource) container.getContainer(), RDF.TYPE, RDF.BAG), is(true));
        // Empty container should have only the type triple
        assertThat(container.getModel().size(), is(1));
    }

    @Test
    void handleEmpty_gatherAsSeq_returnsEmptyTypedContainer() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.SEQ);
        when(gatherMap.getTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.SEQ));
        assertThat(container.getModel().contains((Resource) container.getContainer(), RDF.TYPE, RDF.SEQ), is(true));
        assertThat(container.getModel().size(), is(1));
    }

    @Test
    void handleEmpty_gatherAsAlt_returnsEmptyTypedContainer() {
        // Given
        when(gatherMap.getGathers()).thenReturn(List.of(objectMap));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap)).thenReturn(objectGenerator);
        when(objectGenerator.apply(any(), any())).thenReturn(List.of());
        when(gatherMap.getAllowEmptyListAndContainer()).thenReturn(true);
        when(gatherMap.getGatherAs()).thenReturn(RDF.ALT);
        when(gatherMap.getTargets()).thenReturn(Set.of());

        var generator = RdfListOrContainerGenerator.of(gatherMap, valueFactory, rdfTermGeneratorFactory);

        // When
        var result = generator.apply(null, null);

        // Then
        assertThat(result, hasSize(1));
        var mappedValue = result.get(0);
        assertThat(mappedValue, instanceOf(RdfContainer.class));

        var container = (RdfContainer<Value>) mappedValue;
        assertThat(container.getType(), is(RDF.ALT));
        assertThat(container.getModel().contains((Resource) container.getContainer(), RDF.TYPE, RDF.ALT), is(true));
        assertThat(container.getModel().size(), is(1));
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
}
