package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.LogicalSourceImpl;
import com.taxonic.rml.model.impl.ObjectMapImpl;
import com.taxonic.rml.model.impl.PredicateMapImpl;
import com.taxonic.rml.model.impl.PredicateObjectMapImpl;
import com.taxonic.rml.model.impl.SubjectMapImpl;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.rdf_mapper.Mapper;
import com.taxonic.rml.rdf_mapper.impl.MapperImpl;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlConstantShorthandExpander;
import com.taxonic.rml.vocab.Rdf;

public class TestRdfMapper {

	static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
	static class Example {
		
		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		static final String prefix = "http://data.example.com/";
		
		static final IRI
			MyResource = iri("def/MyResource"),
			when = iri("def/when"),
			description = iri("def/description"),
			accuracy = iri("def/accuracy");
		
	}

	@Test
	public void test() {

		List<TriplesMap> expected = Arrays.asList(
			TriplesMapImpl.newBuilder()
				.logicalSource(
					LogicalSourceImpl.newBuilder()
						.source("source-a.json")
						.iterator("$")
						.referenceFormulation(Rdf.Ql.JsonPath)
						.build()
				)
				.subjectMap(
					SubjectMapImpl.newBuilder()
						.template(TestRdfMapper.Example.prefix + "resource/{id}")
						.clazz(TestRdfMapper.Example.MyResource)
						.build()
				)
				.predicateObjectMap(
					PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
								.constant(TestRdfMapper.Example.when)
								.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
								.reference("when")
								.datatype(XMLSchema.DATE)
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
								.constant(TestRdfMapper.Example.description)
								.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
								.reference("description")
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
								.constant(TestRdfMapper.Example.description)
								.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
								.constant(f.createLiteral("constant description", "en"))
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
								.constant(TestRdfMapper.Example.accuracy)
								.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
								.reference("accuracy")
								.datatype(XMLSchema.FLOAT)
								.build()
						)
						.build()
				)
				.build()
		);
		
		List<TriplesMap> result = loadTriplesMaps("test-a.rml.ttl");
		
		assertEquals(expected, result);
	}
	
	private List<TriplesMap> loadTriplesMaps(String resource) {

		Model originalModel = IoUtils.parse(resource);
		
		Model model = new RmlConstantShorthandExpander().apply(originalModel);
		
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
	
}
