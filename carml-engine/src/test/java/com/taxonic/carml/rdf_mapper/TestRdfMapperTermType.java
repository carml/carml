package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import com.taxonic.carml.model.impl.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;

public class TestRdfMapperTermType extends RmlLoader {

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
	
	@Test
	public void testLoadMappingWithTermTypeLiteral() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
								CarmlPredicateMap.newBuilder()
								.constant(SecondExample.hasBirthday)
								.build()
						)
						.objectMap(
								CarmlObjectMap.newBuilder()
								.reference("birthday")
								.datatype(XMLSchema.DATE)
								.termType(TermType.LITERAL)
								.build()
						)
						.build()
					)
					.build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingLiteral.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeIRI() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.termType(TermType.IRI)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
								CarmlPredicateMap.newBuilder()
								.constant(SecondExample.hasBirthday)
								.termType(TermType.IRI)
								.build()
						)
						.objectMap(
								CarmlObjectMap.newBuilder()
								.constant(SecondExample.Unknown)
								.termType(TermType.IRI)
								.build()
						)
						.build()
					)
					.build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingIRI.rml.ttl");
		assertEquals(result,expected);
	}
	
	@Test
	public void testLoadMappingWithTermTypeBlankNodeB() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.termType(TermType.BLANK_NODE)
							.clazz(SecondExample.Child)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
								CarmlPredicateMap.newBuilder()
								.constant(SecondExample.hasBirthday)
								.build()
						)
						.objectMap(
								CarmlObjectMap.newBuilder()
								.reference("birthday")
								.datatype(XMLSchema.DATE)
								.build()
						)
						.build()
					)
					.build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeB.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeBlankNodeA() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("simple2TestInput.json")
							.iterator("$.Child")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template(SecondExample.prefix + "Child/{first}/{last}")
							.clazz(SecondExample.Child)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
								CarmlPredicateMap.newBuilder()
								.constant(SecondExample.hasBirthday)
								.build()
						)
						.objectMap(
								CarmlObjectMap.newBuilder()
								.termType(TermType.BLANK_NODE)
								.build()
						)
						.build()
					)
					.build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeA.rml.ttl");
		assertEquals(result,expected);
	}
	
}
