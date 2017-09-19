package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.GraphMapImpl;
import com.taxonic.carml.model.impl.JoinImpl;
import com.taxonic.carml.model.impl.LogicalSourceImpl;
import com.taxonic.carml.model.impl.ObjectMapImpl;
import com.taxonic.carml.model.impl.PredicateMapImpl;
import com.taxonic.carml.model.impl.PredicateObjectMapImpl;
import com.taxonic.carml.model.impl.RefObjectMapImpl;
import com.taxonic.carml.model.impl.SubjectMapImpl;
import com.taxonic.carml.model.impl.TriplesMapImpl;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;

public class TestRdfMapper {
	//TODO Add logger stuff
	private static final Logger logger = LoggerFactory.getLogger(TestRdfMapper.class);
	
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
			hasBirthday = iri("hasBirthday"),
			Unknown = iri("Unknown"),
			mainGraph = iri("mainGraph"),
			breakfastItem = iri("ns#breakfastItem"),
			originatesFrom = iri("ns#originatesFrom"),
			Country = iri("ns#Country"),
			officialLanguage = iri("ns#officialLanguage");
	}
	
	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testLoadMappingWithJoinIntegration() {
        logger.info("testing JoinIntegration mapping");
        
		TriplesMapImpl parentTriplesMap = TriplesMapImpl.newBuilder()
			.logicalSource(
				LogicalSourceImpl.newBuilder()
					.source("joinCountries.json")
					.iterator("$")
					.referenceFormulation(Rdf.Ql.JsonPath)
					.build()
			)
			.subjectMap(
				SubjectMapImpl.newBuilder()
					.template("http://country.example.com/{country.name}")
					.clazz(SecondExample.Country)
					.build()
			)
			.predicateObjectMap(
				PredicateObjectMapImpl.newBuilder()
					.predicateMap(
						PredicateMapImpl.newBuilder()
							.constant(SecondExample.officialLanguage)
							.build()
					)
					.objectMap(
							ObjectMapImpl.newBuilder()
							.reference("country.officialLanguage")
							.build()
					)
					.build()
			)
			.build();
		
		List<TriplesMap> expected = Arrays.asList(
				TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("joinBreakfast.xml")
							.iterator("/breakfast-menu/food")
							.referenceFormulation(Rdf.Ql.XPath)
							.build()
					)
					.subjectMap(
						SubjectMapImpl.newBuilder()
							.template("http://food.example.com/{name}")
							.clazz(SecondExample.breakfastItem)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
							.predicateMap(
								PredicateMapImpl.newBuilder()
									.constant(SecondExample.originatesFrom)
									.build()
							)
							.objectMap(
									RefObjectMapImpl.newBuilder()
									.parentTriplesMap(parentTriplesMap)
									.condition(
											JoinImpl.newBuilder()
											.child("/breakfast-menu/food/name")
											.parent("$.country.name")
											.build()
									)
									.build()
								)
							.build()
					)
				.build(),
				parentTriplesMap
			);

		List<TriplesMap> result = loader.load("RdfMapper/test10/joinIntegratedMapping.rml.ttl");
		
		assertEquals(expected, result);
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
		List<TriplesMap> result = loader.load("RdfMapper/test5/languageMapping.rml.ttl");
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
		
		List<TriplesMap> result = loader.load("RdfMapper/test1/constantSubjectShortcutMapping.rml.ttl");
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
		List<TriplesMap> result = loader.load("RdfMapper/test1/constantObjectShortcutMapping.rml.ttl");
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
		List<TriplesMap> result = loader.load("RdfMapper/test10/separateMapsMappingg.rml.ttl");
		assertEquals(expected, result);
		
	}
	
	@Test
	public void testLoadMappingWithParentTriples() {
		TriplesMap parentTriplesMap = TriplesMapImpl.newBuilder()
				.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("parentTriplesTestInput.json")
							.iterator("$.colors.code")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
				.subjectMap(
						SubjectMapImpl.newBuilder()
							.template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
							.clazz(SecondExample.RGBA)
							.build()
					)
				.build();
		
		List<TriplesMap> expected = Arrays.asList(
				(TriplesMapImpl.newBuilder()
					.logicalSource(
						LogicalSourceImpl.newBuilder()
							.source("parentTriplesTestInput.json")
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
									.constant(SecondExample.hasCode)
									.build()
							)
							.objectMap(
									RefObjectMapImpl.newBuilder()
									.parentTriplesMap(
											TriplesMapImpl.newBuilder()
											.logicalSource(
													LogicalSourceImpl.newBuilder()
														.source("parentTriplesTestInput.json")
														.iterator("$.colors.code")
														.referenceFormulation(Rdf.Ql.JsonPath)
														.build()
												)
											.subjectMap(
													SubjectMapImpl.newBuilder()
														.template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
														.clazz(SecondExample.RGBA)
														.build()
												)
											.build()
									)
									.build()
							)
							.build()
						)
					).build(),
				parentTriplesMap			);
		
		List<TriplesMap> result = loader.load("RdfMapper/test9/parentTriplesMapping.rml.ttl");
		assertEquals(result,expected);
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
		
		List<TriplesMap> result = loader.load("RdfMapper/logicalSourceTest.rml.ttl");
		
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
		
		List<TriplesMap> result = loader.load("RdfMapper/test-a.rml.ttl");
		
		assertEquals(expected, result);
	}
	
}
