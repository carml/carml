package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.LogicalSourceImpl;
import com.taxonic.carml.model.impl.ObjectMapImpl;
import com.taxonic.carml.model.impl.PredicateMapImpl;
import com.taxonic.carml.model.impl.PredicateObjectMapImpl;
import com.taxonic.carml.model.impl.SubjectMapImpl;
import com.taxonic.carml.model.impl.TriplesMapImpl;
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
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingLiteral.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeIRI() {
		Set<TriplesMap> expected = ImmutableSet.of(
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
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingIRI.rml.ttl");
		assertEquals(result,expected);
	}
	
	@Test
	public void testLoadMappingWithTermTypeBlankNodeB() {
		Set<TriplesMap> expected = ImmutableSet.of(
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
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeB.rml.ttl");
		assertEquals(result,expected);
	}
	@Test
	public void testLoadMappingWithTermTypeBlankNodeA() {
		Set<TriplesMap> expected = ImmutableSet.of(
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
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeA.rml.ttl");
		assertEquals(result,expected);
	}
	
}
