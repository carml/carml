package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.LogicalSourceImpl;
import com.taxonic.carml.model.impl.ObjectMapImpl;
import com.taxonic.carml.model.impl.PredicateMapImpl;
import com.taxonic.carml.model.impl.PredicateObjectMapImpl;
import com.taxonic.carml.model.impl.SubjectMapImpl;
import com.taxonic.carml.model.impl.TriplesMapImpl;
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
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("simpleTestInput.json")
							.iterator("$")
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
							.constant(SecondExample.language)
							.build()
						)
						.objectMap(
							ObjectMapImpl.newBuilder()
							.reference("language")
							.language("nl")
							.build()
						)
						.build()
					)
				.build()
		);
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test5/languageMapping.rml.ttl");
		assertEquals(result,expected);
	}

	@Test
	public void testLoadMappingWithSubjectConstantShortcut() {
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("constantShortcutMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.constant(SecondExample.Color)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
							.predicateMap(
								PredicateMapImpl.newBuilder()
									.constant(SecondExample.hasHex)
									.build()
							)
							.objectMap(
								ObjectMapImpl.newBuilder()
									.reference("code.hex")
									.build()
							)
						.build()
					)
					.build()
		);
		
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantSubjectShortcutMapping.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithObjectConstantShortcut() {
		List<TriplesMap> expected = Arrays.asList(
				(TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("constantShortcutMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
							SubjectMapImpl.newBuilder()
								.template(SecondExample.prefix + "Color/{color}")
								.build()
					)
					.predicateObjectMap(
							PredicateObjectMapImpl.newBuilder()
								.predicateMap(
									PredicateMapImpl.newBuilder()
										.constant(SecondExample.hasHex)
										.build()
								)
								.objectMap(
									ObjectMapImpl.newBuilder()
										.constant(SecondExample.asciihex)
										.build()
								)
							.build()
					)	
				).build());
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantObjectShortcutMapping.rml.ttl");
		assertEquals(expected,result);
	}
	
	@Test
	public void testLoadMappingWithSeparateMaps() {
		List<TriplesMap> expected = Arrays.asList(
				(TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("SeparateMappingTestInput.json")
							.iterator("$.colors")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.template(SecondExample.prefix + "Color/{color}")
							.clazz(SecondExample.Color)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
							.predicateMap(
								PredicateMapImpl.newBuilder()
								.constant(SecondExample.hasHex)
								.build()
							)
							.objectMap(
								ObjectMapImpl.newBuilder()
								.reference("code.hex")
								.build()
							)
							.build()
					)
				).build()
		);
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/separateMapsMappingg.rml.ttl");
		assertEquals(expected, result);
		
	}
	
	@Test
	public void testLoadMappingWithJustALogicalSource() {

		List<TriplesMap> expected = Arrays.asList(
			TriplesMapImpl.newBuilder()
				.logicalSource(
					LogicalSourceImpl.newBuilder()
						.source("test-source.json")
						.iterator("$")
						.referenceFormulation(Rdf.Ql.JsonPath)
						.build()
				)
				.build()
		);
		
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/logicalSourceTest.rml.ttl");
		
		assertEquals(expected, result);
		
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
						.template(Example.prefix + "resource/{id}")
						.clazz(Example.MyResource)
						.build()
				)
				.predicateObjectMap(
					PredicateObjectMapImpl.newBuilder()
						.predicateMap(
							PredicateMapImpl.newBuilder()
								.constant(Example.when)
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
								.constant(TestRdfMapperBasic.Example.description)
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
								.constant(TestRdfMapperBasic.Example.description)
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
								.constant(TestRdfMapperBasic.Example.accuracy)
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
		
		List<TriplesMap> result = loadRmlFromTtl("RdfMapper/test-a.rml.ttl");
		
		assertEquals(expected, result);
	}
	
}
