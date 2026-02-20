package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.carml.engine.TermGenerator;
import io.carml.model.GatherMap;
import io.carml.model.ObjectMap;
import io.carml.model.SubjectMap;
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
