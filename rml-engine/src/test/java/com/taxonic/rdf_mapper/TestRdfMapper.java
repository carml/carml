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
import org.junit.Test;

import com.taxonic.rdf_mapper.impl.MapperImpl;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.LogicalSourceImpl;
import com.taxonic.rml.model.impl.ObjectMapImpl;
import com.taxonic.rml.model.impl.PredicateObjectMapImpl;
import com.taxonic.rml.model.impl.SubjectMapImpl;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.vocab.Rml;

public class TestRdfMapper {

	private static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
	private static final IRI logicalSource = f.createIRI(Rml.logicalSource);
	
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
					"http://data.example.com/resource/{id}",
					null,
					null,
					set(f.createIRI("http://data.example.com/def/MyResource"))
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
								f.createIRI("http://www.w3.org/2001/XMLSchema#date"),
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
								f.createIRI("http://www.w3.org/2001/XMLSchema#float"),
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
			.filter(null, logicalSource, null)
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
