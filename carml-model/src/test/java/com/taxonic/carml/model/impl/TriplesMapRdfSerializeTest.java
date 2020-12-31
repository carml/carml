package com.taxonic.carml.model.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.RmlMappingLoader;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TriplesMapRdfSerializeTest {

  private RmlMappingLoader mappingLoader;

  @BeforeEach
  public void init() {
    mappingLoader = RmlMappingLoader.build();
  }

  @Test
  public void TriplesMapAsRdfRoundTripTest() {
    InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("Mapping.rml.ttl");
    Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

    Model model = mapping.stream()
        .map(Resource::asRdf)
        .flatMap(Model::stream)
        .collect(Collectors.toCollection(LinkedHashModel::new));

    Set<TriplesMap> mappingReloaded = mappingLoader.load(model);

    Model modelReloaded = mappingReloaded.stream()
        .map(Resource::asRdf)
        .flatMap(Model::stream)
        .collect(Collectors.toCollection(LinkedHashModel::new));

    assertThat(model, is(modelReloaded));
  }

}
