package io.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlJoin;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlPredicateMap;
import io.carml.model.impl.CarmlPredicateObjectMap;
import io.carml.model.impl.CarmlRefObjectMap;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.model.impl.CarmlTriplesMap;
import io.carml.vocab.Rdf;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class TestRdfMapperParentTriplesMap extends RmlLoader {

  static final SimpleValueFactory f = SimpleValueFactory.getInstance();

  static class SecondExample {

    static IRI iri(String suffix) {
      return f.createIRI(prefix + suffix);
    }

    static final String prefix = "http://example.com/";

    static final IRI RGBA = iri("RGBA");

    static final IRI Color = iri("Color");

    static final IRI hasCode = iri("hasCode");

    static final IRI hasHex = iri("hasHex");

    static final IRI asciihex = f.createIRI("http://www.asciitable.com/hex");

    static final IRI breakfastItem = iri("ns#breakfastItem");

    static final IRI originatesFrom = iri("ns#originatesFrom");

    static final IRI Country = iri("ns#Country");

    static final IRI officialLanguage = iri("ns#officialLanguage");
  }

  // @Test
  public void testLoadMappingWithJoinIntegration() {

    CarmlTriplesMap parentTriplesMap = CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("joinCountries.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template("http://country.example.com/{country.name}")
            .clazz(SecondExample.Country)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.officialLanguage)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("country.officialLanguage")
                .build())
            .build())
        .build();

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("joinBreakfast.xml")
            .iterator("/breakfast-menu/food")
            .referenceFormulation(Rdf.Ql.XPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template("http://food.example.com/{name}")
            .clazz(SecondExample.breakfastItem)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.originatesFrom)
                .build())
            .objectMap(CarmlRefObjectMap.builder()
                .parentTriplesMap(parentTriplesMap)
                .joinCondition(CarmlJoin.builder()
                    .child("/breakfast-menu/food/name")
                    .parent("$.country.name")
                    .build())
                .build())
            .build())
        .build(), parentTriplesMap);

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/joinIntegratedMapping.rml.ttl");

    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithParentTriples() {
    TriplesMap parentTriplesMap = CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("parentTriplesTestInput.json")
            .iterator("$.colors.code")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
            .clazz(SecondExample.RGBA)
            .build())
        .build();

    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("parentTriplesTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Color/{color}")
            .clazz(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasCode)
                .build())
            .objectMap(CarmlRefObjectMap.builder()
                .parentTriplesMap(CarmlTriplesMap.builder()
                    .logicalSource(CarmlLogicalSource.builder()
                        .source("parentTriplesTestInput.json")
                        .iterator("$.colors.code")
                        .referenceFormulation(Rdf.Ql.JsonPath)
                        .build())
                    .subjectMap(CarmlSubjectMap.builder()
                        .template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
                        .clazz(SecondExample.RGBA)
                        .build())
                    .build())
                .build())
            .build())).build(),
        parentTriplesMap);

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test9/parentTriplesMapping.rml.ttl");

    assertThat(result, is(expected));
  }

}
