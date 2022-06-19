package io.carml.rdfmapper;

import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import java.util.Set;
import org.eclipse.rdf4j.rio.RDFFormat;

class RmlLoader {

  private RmlMappingLoader loader = RmlMappingLoader.build();

  protected Set<TriplesMap> loadRmlFromTtl(String resource) {
    return loader.load(RDFFormat.TURTLE, resource);
  }

}
