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
