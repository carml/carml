package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.GraphMapImpl;
import com.taxonic.carml.model.impl.LogicalSourceImpl;
import com.taxonic.carml.model.impl.ObjectMapImpl;
import com.taxonic.carml.model.impl.PredicateMapImpl;
import com.taxonic.carml.model.impl.PredicateObjectMapImpl;
import com.taxonic.carml.model.impl.SubjectMapImpl;
import com.taxonic.carml.model.impl.TriplesMapImpl;
import com.taxonic.carml.vocab.Rdf;

public class TestRdfMapperGraphMaps extends RmlLoader {

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
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingPredObj.rml.ttl");
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
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingSubjectB.rml.ttl");
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
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingSubjectA.rml.ttl");
		assertEquals(expected,result);
	}
	
}
