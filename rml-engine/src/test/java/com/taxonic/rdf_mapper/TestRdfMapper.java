package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.rdf_mapper.impl.MapperImpl;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.LogicalSourceImpl;
import com.taxonic.rml.model.impl.ObjectMapImpl;
import com.taxonic.rml.model.impl.PredicateObjectMapImpl;
import com.taxonic.rml.model.impl.SubjectMapImpl;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.util.IoUtils;

public class TestRdfMapper {

	private static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
	private static class Rdf {
		
		// TODO not too happy about redefining the vocabs here. in pkg "vocab",
		// they're defined as Strings. here, they're redefined in the RDF4J model API.
		
		static class Rml {
			
			static IRI iri(String suffix) { return f.createIRI(prefix + suffix); }
			
			static final String prefix = "http://semweb.mmlab.be/ns/rml#";
			
			static final IRI logicalSource = iri("logicalSource");
			
		}
		
		static class Example {
			
			static IRI iri(String suffix) { return f.createIRI(prefix + suffix); }
			
			static final String prefix = "http://data.example.com/";
			
			static final IRI MyResource = iri("def/MyResource");
			
		}
	}
	
	@Test
	public void test() {

		// TODO use builder pattern
		
		List<TriplesMap> expected = Arrays.asList(
			new TriplesMapImpl(
				new LogicalSourceImpl(
					"source-a.json",
					"$",
					f.createIRI("http://semweb.mmlab.be/ns/ql#JSONPath")
				),
				new SubjectMapImpl(
					null,
					null,
					Rdf.Example.prefix + "resource/{id}",
					null,
					null,
					set(Rdf.Example.MyResource)
				),
				set(
					new PredicateObjectMapImpl(
						set(), // TODO
						set(
							new ObjectMapImpl(
								"when",
								null,
								null,
								null,
								null,
								XMLSchema.DATE,
								null
							)
						)
					),
					new PredicateObjectMapImpl(
						set(), // TODO
						set(
							new ObjectMapImpl(
								"description",
								null,
								null,
								null,
								null,
								null,
								null
							)
						)
					),
					new PredicateObjectMapImpl(
						set(), // TODO
						set(
							new ObjectMapImpl(
								"accuracy",
								null,
								null,
								null,
								null,
								XMLSchema.FLOAT,
								null
							)
						)
					)
				)
			)
		);
		
		List<TriplesMap> result = loadTriplesMaps("test-a.rml.ttl");
		
		assertEquals(expected, result);
	}
	
	private List<TriplesMap> loadTriplesMaps(String resource) {

		Model model = IoUtils.parse(resource);
		
		Mapper mapper = new MapperImpl();
		
		return
		model
			.filter(null, Rdf.Rml.logicalSource, null)
			.subjects()
			.stream()
			.map(r -> {
				
				System.out.println("found triples map resource: " + r);
				
				TriplesMap map = mapper.map(model, r, TriplesMapImpl.class);
				
				System.out.println("triples map object model:\n" + map);
				
				return map;
			})
			.collect(Collectors.toList());		
	}
	
	private static <T> Set<T> set(T... elements) {
		return new LinkedHashSet<>(Arrays.asList(elements));
	}

}
