package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

class MappingsTest {

  private final RmlMappingLoader mappingLoader = RmlMappingLoader.build();

  @Test
  void given_whenFilterMappable_then() {
    // Given
    InputStream mappingSource = MappingsTest.class.getResourceAsStream("Mapping.rml.ttl");
    Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

    // When
    var filtered = Mappings.filterMappable(mapping);

    // Then
    var filteredResourceIds = filtered.stream()
        .map(Resource::getId)
        .collect(Collectors.toSet());

    assertThat(filteredResourceIds,
        containsInAnyOrder("http://example.org/TriplesMap", "http://example.org/FileSourceTM"));
  }
}
