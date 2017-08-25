package com.taxonic.rml.util;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.rdf_mapper.impl.MappingCache;
import com.taxonic.rml.rdf_mapper.util.RdfObjectLoader;
import com.taxonic.rml.vocab.Rdf;
import com.taxonic.rml.vocab.Rdf.Rr;

public class RmlMappingLoader {
	
	private RmlConstantShorthandExpander shorthandExpander;
	
	public RmlMappingLoader(
		RmlConstantShorthandExpander shorthandExpander
	) {
		this.shorthandExpander = shorthandExpander;
	}

	// TODO: PM: shouldn't the return type be Set?
	public List<TriplesMap> load(String resource) {

		Model originalModel = IoUtils.parse(resource);
		
		return 
			ImmutableList.copyOf(
				RdfObjectLoader.load(
					selectTriplesMaps, 
					TriplesMapImpl.class, 
					originalModel, 
					shorthandExpander,
					this::addTermTypes
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
