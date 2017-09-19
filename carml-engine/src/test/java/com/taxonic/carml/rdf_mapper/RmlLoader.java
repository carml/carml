package com.taxonic.carml.rdf_mapper;

import java.util.Set;

import org.eclipse.rdf4j.rio.RDFFormat;

import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.RmlMappingLoader;

class RmlLoader {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	protected Set<TriplesMap> loadRmlFromTtl(String resource) {
		return loader.load(resource, RDFFormat.TURTLE);
	}

}
