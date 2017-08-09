package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.GraphMapImpl;
import com.taxonic.rml.model.impl.JoinImpl;
import com.taxonic.rml.model.impl.LogicalSourceImpl;
import com.taxonic.rml.model.impl.ObjectMapImpl;
import com.taxonic.rml.model.impl.PredicateMapImpl;
import com.taxonic.rml.model.impl.PredicateObjectMapImpl;
import com.taxonic.rml.model.impl.RefObjectMapImpl;
import com.taxonic.rml.model.impl.SubjectMapImpl;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.util.RmlMappingLoader;
import com.taxonic.rml.vocab.Rdf;
import com.taxonic.rml.vocab.Rdf.Rr;

public class TestRdfMapperGraphMaps {

	static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
	static class SecondExample {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		static final String prefix = "http://example.com/";

		static final IRI
			Child = iri("Child"),
			language = iri("language"),
			hasBirthday = iri("hasBirthday"),
			mainGraph = iri("mainGraph");
	}
	
	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testLoadMappingWithGraphMapsPredicateObject() {
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
							.constant(SecondExample.hasBirthday)
							.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
							.reference("birthday")
							.build()
						)
						.graphMap(
							GraphMapImpl.newBuilder()
							.template("http://example.com/graphID/{BSN}")
							.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test15/graphMapMappingPredObj.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithGraphMapsSubjectB() {
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.graphMap(
									GraphMapImpl.newBuilder()
									.constant(SecondExample.mainGraph)
									.build()
							)
							.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test15/graphMapMappingSubjectB.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithGraphMapsSubjectA() {
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.graphMap(
									GraphMapImpl.newBuilder()
									.template("http://example.com/graphID/{BSN}")
									.build()
							)
							.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test15/graphMapMappingSubjectA.rml.ttl");
		assertEquals(expected,result);
	}
	
}
