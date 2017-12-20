package com.taxonic.carml.rdf_mapper;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import com.taxonic.carml.model.impl.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlJoin;
import com.taxonic.carml.vocab.Rdf;

public class TestRdfMapperParentTriplesMap extends RmlLoader {

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
	
	@Test
	public void testLoadMappingWithJoinIntegration() {
		
		CarmlTriplesMap parentTriplesMap = CarmlTriplesMap.newBuilder()
			.logicalSource(
				CarmlLogicalSource.newBuilder()
					.source("joinCountries.json")
					.iterator("$")
					.referenceFormulation(Rdf.Ql.JsonPath)
					.build()
			)
			.subjectMap(
				CarmlSubjectMap.newBuilder()
					.template("http://country.example.com/{country.name}")
					.clazz(SecondExample.Country)
					.build()
			)
			.predicateObjectMap(
				CarmlPredicateObjectMap.newBuilder()
					.predicateMap(
						CarmlPredicateMap.newBuilder()
							.constant(SecondExample.officialLanguage)
							.build()
					)
					.objectMap(
							CarmlObjectMap.newBuilder()
							.reference("country.officialLanguage")
							.build()
					)
					.build()
			)
			.build();
		
		Set<TriplesMap> expected = ImmutableSet.of(
				CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("joinBreakfast.xml")
							.iterator("/breakfast-menu/food")
							.referenceFormulation(Rdf.Ql.XPath)
							.build()
					)
					.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template("http://food.example.com/{name}")
							.clazz(SecondExample.breakfastItem)
							.build()
					)
					.predicateObjectMap(
						CarmlPredicateObjectMap.newBuilder()
							.predicateMap(
								CarmlPredicateMap.newBuilder()
									.constant(SecondExample.originatesFrom)
									.build()
							)
							.objectMap(
									CarmlRefObjectMap.newBuilder()
									.parentTriplesMap(parentTriplesMap)
									.condition(
											CarmlJoin.newBuilder()
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

		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/joinIntegratedMapping.rml.ttl");
		
		assertEquals(expected, result);
	}
	
	@Test
	public void testLoadMappingWithParentTriples() {
		TriplesMap parentTriplesMap = CarmlTriplesMap.newBuilder()
				.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("parentTriplesTestInput.json")
							.iterator("$.colors.code")
							.referenceFormulation(Rdf.Ql.JsonPath)
							.build()
					)
				.subjectMap(
						CarmlSubjectMap.newBuilder()
							.template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
							.clazz(SecondExample.RGBA)
							.build()
					)
				.build();
		
		Set<TriplesMap> expected = ImmutableSet.of(
				(CarmlTriplesMap.newBuilder()
					.logicalSource(
						CarmlLogicalSource.newBuilder()
							.source("parentTriplesTestInput.json")
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
									.constant(SecondExample.hasCode)
									.build()
							)
							.objectMap(
									CarmlRefObjectMap.newBuilder()
									.parentTriplesMap(
											CarmlTriplesMap.newBuilder()
											.logicalSource(
													CarmlLogicalSource.newBuilder()
														.source("parentTriplesTestInput.json")
														.iterator("$.colors.code")
														.referenceFormulation(Rdf.Ql.JsonPath)
														.build()
												)
											.subjectMap(
													CarmlSubjectMap.newBuilder()
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
		
		Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test9/parentTriplesMapping.rml.ttl");
		assertEquals(result,expected);
	}
	
}
