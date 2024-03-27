package io.carml.model.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import java.io.InputStream;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled // TODO add new constructs
class TriplesMapRdfSerializeTest {

    private RmlMappingLoader mappingLoader;

    @BeforeEach
    public void init() {
        mappingLoader = RmlMappingLoader.build();
    }

    @Test
    void triplesMapAsRdfRoundTripTest() {
        InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("Mapping.rml.ttl");
        Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

        Model model =
                mapping.stream().map(Resource::asRdf).flatMap(Model::stream).collect(ModelCollector.toModel());

        Set<TriplesMap> mappingReloaded = mappingLoader.load(model);

        Model modelReloaded = mappingReloaded.stream()
                .map(Resource::asRdf)
                .flatMap(Model::stream)
                .collect(ModelCollector.toModel());

        assertThat(model, is(modelReloaded));
    }
}
