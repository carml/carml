package com.taxonic.carml.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.rio.RDFFormat;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlStream;
import com.taxonic.carml.model.impl.TriplesMapImpl;
import com.taxonic.carml.rdf_mapper.impl.MappingCache;
import com.taxonic.carml.rdf_mapper.util.RdfObjectLoader;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rdf.Rr;

public class RmlMappingLoader {
	
	private RmlConstantShorthandExpander shorthandExpander;
	
	public RmlMappingLoader(
		RmlConstantShorthandExpander shorthandExpander
	) {
		this.shorthandExpander = shorthandExpander;
	}

	public Set<TriplesMap> load(String resource, RDFFormat rdfFormat) {
		InputStream input = RmlMappingLoader.class.getClassLoader().getResourceAsStream(resource);		
		return load(input, rdfFormat);
	}
	
	public Set<TriplesMap> load(Path pathToFile, RDFFormat rdfFormat) {
		InputStream input;
		try {
			input = Files.newInputStream(pathToFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
		return load(input, rdfFormat);
	}
	
	public Set<TriplesMap> load(InputStream input, RDFFormat rdfFormat) {
		// TODO: PM do we really need IoUtils?
		Model originalModel = IoUtils.parse(input, rdfFormat);
		return
			ImmutableSet.<TriplesMap>copyOf(
				RdfObjectLoader.load(
					selectTriplesMaps, 
					TriplesMapImpl.class, 
					originalModel, 
					shorthandExpander,
					this::addTermTypes,
					m -> m.addDecidableType(Rdf.Carml.Stream, CarmlStream.class)
				)
			);
	}
	
	private void addTermTypes(MappingCache cache) {
		class AddTermTypes {
			
			void add(IRI iri, TermType termType) {
				cache.addCachedMapping(iri, TermType.class, termType);
			}
			
			void run() {
				add(Rr.BlankNode, TermType.BLANK_NODE);
				add(Rr.IRI      , TermType.IRI       );
				add(Rr.Literal  , TermType.LITERAL   );
			}
		}
		new AddTermTypes().run();
	}
	
	public static RmlMappingLoader build() {
		return new RmlMappingLoader(
			new RmlConstantShorthandExpander()
		);
	}
	
	private static Function<Model, Set<Resource>> selectTriplesMaps = 
		model ->
			ImmutableSet.copyOf(
				model
				.filter(null, Rdf.Rml.logicalSource, null)
				.subjects()
			);
}
