package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import com.taxonic.carml.model.impl.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;

public class TestRdfMapperBasic extends RmlLoader {

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
	
	static class SecondExample {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		static final String prefix = "http://example.com/";

		static final IRI
			RGBA = iri("RGBA"),
			Color = iri("Color"),
			hasCode = iri("hasCode"),
			hasHex = iri("hasHex"),
			asciihex = f.createIRI("http://www.asciitable.com/hex"),
			Child = iri("Child"),
			language = iri("language"),
			hasBirthday = iri("hasBirthday");
	}
	
	@Test
	public void testLoadMappingWithLanguage() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("simpleTestInput.json")
							.iterator("$")
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
							.constant(SecondExample.language)
							.build()
						)
						.objectMap(
							CarmlObjectMap.newBuilder()
							.reference("language")
							.language("nl")
							.build()
						)
						.build()
					)
				.build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test5/languageMapping.rml.ttl");
		assertEquals(result,expected);
	}

	@Test
	public void testLoadMappingWithSubjectConstantShortcut() {
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("constantShortcutMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.constant(SecondExample.Color)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
							.predicateMap(
								CarmlPredicateMap.newBuilder()
									.constant(SecondExample.hasHex)
									.build()
							)
							.objectMap(
								CarmlObjectMap.newBuilder()
									.reference("code.hex")
									.build()
							)
						.build()
					)
					.build()
		);
		
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantSubjectShortcutMapping.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithObjectConstantShortcut() {
		Set<TriplesMap> expected = ImmutableSet.of(
				(CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("constantShortcutMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
							CarmlSubjectMap.newBuilder()
								.template(SecondExample.prefix + "Color/{color}")
								.build()
					)
					.predicateObjectMap(
							CarmlPredicateObjectMap.newBuilder()
								.predicateMap(
									CarmlPredicateMap.newBuilder()
										.constant(SecondExample.hasHex)
										.build()
								)
								.objectMap(
									CarmlObjectMap.newBuilder()
										.constant(SecondExample.asciihex)
										.build()
								)
							.build()
					)	
				).build());
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantObjectShortcutMapping.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithSeparateMaps() {
		Set<TriplesMap> expected = ImmutableSet.of(
				(CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("SeparateMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template(SecondExample.prefix + "Color/{color}")
							.clazz(SecondExample.Color)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
							.predicateMap(
								CarmlPredicateMap.newBuilder()
								.constant(SecondExample.hasHex)
								.build()
							)
							.objectMap(
								CarmlObjectMap.newBuilder()
								.reference("code.hex")
								.build()
							)
							.build()
					)
				).build()
		);
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/separateMapsMappingg.rml.ttl");
		assertEquals(expected, result);
		
	}
	
	@Test
	public void testLoadMappingWithJustALogicalSource() {

		Set<TriplesMap> expected = ImmutableSet.of(
			CarmlTriplesMap.newBuilder()
				.logicalSource(
					CarmlLogicalSource.newBuilder()
						.source("test-source.json")
						.iterator("$")
						.referenceFormulation(Rdf.Ql.JsonPath)
						.build()
				)
				.build()
		);
		
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/logicalSourceTest.rml.ttl");
		
		assertEquals(expected, result);
		
	}
	
	@Test
	public void test() {

		Set<TriplesMap> expected = ImmutableSet.of(
			CarmlTriplesMap.newBuilder()
				.logicalSource(
					CarmlLogicalSource.newBuilder()
						.source("source-a.json")
						.iterator("$")
						.referenceFormulation(Rdf.Ql.JsonPath)
						.build()
				)
				.subjectMap(
					CarmlSubjectMap.newBuilder()
						.template(Example.prefix + "resource/{id}")
						.clazz(Example.MyResource)
						.build()
				)
				.predicateObjectMap(
					CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
							CarmlPredicateMap.newBuilder()
								.constant(Example.when)
								.build()
						)
						.objectMap(
							CarmlObjectMap.newBuilder()
								.reference("when")
								.datatype(XSD.DATE)
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
							CarmlPredicateMap.newBuilder()
								.constant(TestRdfMapperBasic.Example.description)
								.build()
						)
						.objectMap(
							CarmlObjectMap.newBuilder()
								.reference("description")
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
							CarmlPredicateMap.newBuilder()
								.constant(TestRdfMapperBasic.Example.description)
								.build()
						)
						.objectMap(
							CarmlObjectMap.newBuilder()
								.constant(f.createLiteral("constant description", "en"))
								.build()
						)
						.build()
				)
				.predicateObjectMap(
					CarmlPredicateObjectMap.newBuilder()
						.predicateMap(
							CarmlPredicateMap.newBuilder()
								.constant(TestRdfMapperBasic.Example.accuracy)
								.build()
						)
						.objectMap(
							CarmlObjectMap.newBuilder()
								.reference("accuracy")
								.datatype(XSD.FLOAT)
								.build()
						)
						.build()
				)
				.build()
		);
		
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test-a.rml.ttl");
		
		assertEquals(expected, result);
	}
	
}
