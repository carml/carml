package com.taxonic.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.taxonic.rml.model.TriplesMap;
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

public class TestRdfMapperParentTriplesMap {

	static final SimpleValueFactory f = SimpleValueFactory.getInstance();
	
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
			breakfastItem = iri("ns#breakfastItem"),
			originatesFrom = iri("ns#originatesFrom"),
			Country = iri("ns#Country"),
			officialLanguage = iri("ns#officialLanguage");
	}
	
	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testLoadMappingWithJoinIntegration() {
		
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
	
}
