package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.LogicalSourceImpl;
import com.taxonic.rml.model.impl.ObjectMapImpl;
import com.taxonic.rml.model.impl.PredicateMapImpl;
import com.taxonic.rml.model.impl.PredicateObjectMapImpl;
import com.taxonic.rml.model.impl.SubjectMapImpl;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.util.RmlMappingLoader;
import com.taxonic.rml.vocab.Rdf;

public class TestRdfMapperTermType {

	static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
	static class SecondExample {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		static final String prefix = "http://example.com/";

		static final IRI
			Unknown = iri("Unknown"),
			Child = iri("Child"),
			language = iri("language"),
			hasBirthday = iri("hasBirthday");
	}
	
	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testLoadMappingWithTermTypeLiteral() {
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
								.datatype(XMLSchema.DATE)
								.termType(TermType.LITERAL)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test14/termTypeMappingLiteral.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeIRI() {
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
							.termType(TermType.IRI)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
						.predicateMap(
								PredicateMapImpl.newBuilder()
								.constant(SecondExample.hasBirthday)
								.termType(TermType.IRI)
								.build()
						)
						.objectMap(
								ObjectMapImpl.newBuilder()
								.constant(SecondExample.Unknown)
								.termType(TermType.IRI)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test14/termTypeMappingIRI.rml.ttl");
		assertEquals(result,expected);
	}
	
	@Test
	public void testLoadMappingWithTermTypeBlankNodeB() {
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
							.termType(TermType.BLANK_NODE)
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
								.datatype(XMLSchema.DATE)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test14/termTypeMappingBlankNodeB.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeBlankNodeA() {
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
								.termType(TermType.BLANK_NODE)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loader.load("RdfMapper/test14/termTypeMappingBlankNodeA.rml.ttl");
		assertEquals(result,expected);
	}
	
}
