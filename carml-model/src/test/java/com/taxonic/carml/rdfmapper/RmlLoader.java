package com.taxonic.carml.rdfmapper;

import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.RmlMappingLoader;
import java.util.Set;
import org.eclipse.rdf4j.rio.RDFFormat;

class RmlLoader {

  private RmlMappingLoader loader = RmlMappingLoader.build();

  protected Set<TriplesMap> loadRmlFromTtl(String resource) {
    return loader.load(RDFFormat.TURTLE, resource);
  }

}
