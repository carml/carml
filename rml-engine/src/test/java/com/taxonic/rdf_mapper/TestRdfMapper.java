package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.GraphMapImpl;
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
import com.taxonic.rml.vocab.Rdf.Rr;

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
			mainGraph = iri("mainGraph");
	}
	
	@Test
	public void testLoadMappingWithJoinIntegration() {
		List<TriplesMap> expected = null;
		List<TriplesMap> result = loadTriplesMaps("test10/joinIntegratedMapping.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test15/graphMapMappingPredObj.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test15/graphMapMappingSubjectB.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test15/graphMapMappingSubjectA.rml.ttl");
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
								.termType(Rr.Literal)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loadTriplesMaps("test14/termTypeMappingLiteral.rml.ttl");
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
							.termType(Rr.IRI)
							.build()
					)
					.predicateObjectMap(
						PredicateObjectMapImpl.newBuilder()
						.predicateMap(
								PredicateMapImpl.newBuilder()
								.constant(SecondExample.hasBirthday)
								.termType(Rr.IRI)
								.build()
						)
						.objectMap(
								ObjectMapImpl.newBuilder()
								.constant(SecondExample.Unknown)
								.termType(Rr.IRI)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loadTriplesMaps("test14/termTypeMappingIRI.rml.ttl");
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
							.termType(Rr.BlankNode)
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
		List<TriplesMap> result = loadTriplesMaps("test14/termTypeMappingBlankNodeB.rml.ttl");
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
								.termType(Rr.BlankNode)
								.build()
						)
						.build()
					)
					.build()
		);
		List<TriplesMap> result = loadTriplesMaps("test14/termTypeMappingBlankNodeA.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test5/languageMapping.rml.ttl");
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
		
		List<TriplesMap> result = loadTriplesMaps("test1/constantSubjectShortcutMapping.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test1/constantObjectShortcutMapping.rml.ttl");
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
		List<TriplesMap> result = loadTriplesMaps("test10/separateMapsMappingg.rml.ttl");
		assertEquals(expected, result);
		
	}
	
	@Test
	public void testLoadMappingWithParentTriples() {
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
									ObjectMapImpl.newBuilder().build()
								)
							.build()
					)
				).build(),
				//TO DO Instead of using another TriplesMap, use a referencing object map instead.
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
		);
		
		List<TriplesMap> result = loadTriplesMaps("test9/parentTriplesMapping.rml.ttl");
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
		
		List<TriplesMap> result = loadTriplesMaps("logicalSourceTest.rml.ttl");
		
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
