import io.carml.model.FilePath;
import io.carml.model.Template;
import io.carml.model.impl.*;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.util.RmlNamespaces;
import io.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class RdfRmlLoaderTest {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private Model model;


    public void prepareTest(String resource, RDFFormat rdfFormat) {
        try (InputStream input = RdfRmlLoaderTest.class.getResourceAsStream(resource)) {
            model = Rio.parse(input, "", rdfFormat);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void givenSubjectMapInTurtle_whenMap_thenReturnMappedSubjectMapWithTemplate() {
        // Given
        prepareTest("mapping.ttl", RDFFormat.TURTLE);

        // When
        var subjects = RdfObjectLoader.load(
                model -> Set.of(VF.createIRI("http://example.com/base/NameSubjectMap")),
                CarmlSubjectMap.class,
                model,
                m -> m,
                mappingCache -> {
                },
                mapper -> {
                },
                RmlNamespaces.RML_NAMESPACES);

        // Then
        assertThat(subjects, hasSize(1));
        var namedSubjectMap = subjects.iterator().next();
        assertThat(namedSubjectMap.getTemplate(), isA(Template.class));

        var segments = namedSubjectMap.getTemplate().getSegments();
        assertThat(segments, hasSize(2));
        Template.Segment firstSegment = segments.get(0);
        Template.Segment secondSegment = segments.get(1);

        assertThat(firstSegment, isA(CarmlTemplate.TextSegment.class));
        assertThat(firstSegment.getValue(), is("http://example.com/"));

        assertThat(secondSegment, isA(CarmlTemplate.ExpressionSegment.class));
        assertThat(secondSegment.getValue(), is("$.Name"));
    }


    @Test
    void givenObjectMapInTurtle_whenMap_thenReturnMappedObjectMapWithReference() {
        // Given
        prepareTest("mapping.ttl", RDFFormat.TURTLE);

        // When
        var subjects = RdfObjectLoader.load(
                model -> Set.of(VF.createIRI("http://example.com/base/NameObjectMap")),
                CarmlObjectMap.class,
                model,
                m -> m,
                mappingCache -> {
                },
                mapper -> {
                },
                RmlNamespaces.RML_NAMESPACES);

        // Then
        assertThat(subjects, hasSize(1));
        var namedSubjectMap = subjects.iterator().next();
        assertThat(namedSubjectMap.getReference(), is("$.Name"));
    }

    @Test
    void givenRmlMapping_whenMap_thenReturnMappedCarmlModelObjects() {
        // Given
        prepareTest("mapping.ttl", RDFFormat.TURTLE);

        // When
        var triplesMap = RdfObjectLoader.load(
                model -> Set.of(VF.createIRI("http://example.com/base/TriplesMap1")),
                CarmlTriplesMap.class,
                model,
                m -> m,
                mappingCache -> {
                },
                mapper -> mapper.addDecidableType(Rdf.Rml.FilePath, FilePath.class)
                        .bindInterfaceImplementation(FilePath.class, CarmlFilePath.class),
                RmlNamespaces.RML_NAMESPACES);

        // Then
        assertThat(triplesMap, hasSize(1));
        var tm = triplesMap.iterator().next();
        assertThat(tm.getLogicalSource(), is(notNullValue()));
        assertThat(tm.getSubjectMaps(), hasSize(1));
        var subjectMap = tm.getSubjectMaps().iterator().next();
        assertThat(subjectMap.getTemplate(), notNullValue());

        // Check predicate object map with reference value
        assertThat(tm.getPredicateObjectMaps(), hasSize(1));
        var predicateObjectMap = tm.getPredicateObjectMaps().iterator().next();
        assertThat(predicateObjectMap.getObjectMaps(), hasSize(1));
        var objectMap = predicateObjectMap.getObjectMaps().iterator().next();
        assertThat(objectMap, notNullValue());
    }
}
