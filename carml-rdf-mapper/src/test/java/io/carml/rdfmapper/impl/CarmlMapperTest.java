package io.carml.rdfmapper.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Test;

@SuppressWarnings("java:S1135")
class CarmlMapperTest {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private Model model;

    public void prepareTest(String resource, RDFFormat rdfFormat) {
        try (InputStream input = CarmlMapperTest.class.getResourceAsStream(resource)) {
            model = Rio.parse(input, "", rdfFormat);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void givenMethodWithMultiplePropertyAnnotations_whenMap_thenReturnsExpectedObject() {
        // Given
        prepareTest("Person.ld.json", RDFFormat.JSONLD);
        var mapper = new CarmlMapper();

        // When
        Person manu = mapper.map(model, VF.createIRI("http://example.org/people#manu"), Set.of(Person.class));

        // Then
        var acquaintances = manu.getKnows();
        assertThat(acquaintances, hasSize(6));
    }

    @Test
    void givenInputWithCollectionResource_whenMap_thenReturnMappedCollection() {
        // Given
        prepareTest("Person.ttl", RDFFormat.TURTLE);
        var mapper = new CarmlMapper();

        // When
        Person manu = mapper.map(model, VF.createIRI("http://example.org/people#manu"), Set.of(Person.class));

        // Then
        var friends = manu.getFriends();
        assertThat(friends, hasSize(3));
    }

    void mapper_ifContainsCachedMappings_UsesThem() {
        // TODO
    }

    // TODO: PM: test mapping of relevant RDF constructs, this should replace the RDF Mapper tests in
    // RML Engine

}
