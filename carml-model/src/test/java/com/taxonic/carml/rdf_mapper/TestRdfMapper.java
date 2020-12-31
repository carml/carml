package com.taxonic.carml.rdf_mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlGraphMap;
import com.taxonic.carml.model.impl.CarmlJoin;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.model.impl.CarmlObjectMap;
import com.taxonic.carml.model.impl.CarmlPredicateMap;
import com.taxonic.carml.model.impl.CarmlPredicateObjectMap;
import com.taxonic.carml.model.impl.CarmlRefObjectMap;
import com.taxonic.carml.model.impl.CarmlSubjectMap;
import com.taxonic.carml.model.impl.CarmlTriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRdfMapper extends RmlLoader {
  // TODO Add logger stuff
  private static final Logger logger = LoggerFactory.getLogger(TestRdfMapper.class);

  static final SimpleValueFactory f = SimpleValueFactory.getInstance();

  static class Example {

    static IRI iri(String suffix) {
      return f.createIRI(prefix + suffix);
    }

    static final String prefix = "http://data.example.com/";

    static final IRI MyResource = iri("def/MyResource"), when = iri("def/when"), description = iri("def/description"),
        accuracy = iri("def/accuracy");

  }

  static class SecondExample {

    static IRI iri(String suffix) {
      return f.createIRI(prefix + suffix);
    }

    static final String prefix = "http://example.com/";

    static final IRI RGBA = iri("RGBA"), Color = iri("Color"), hasCode = iri("hasCode"), hasHex = iri("hasHex"),
        asciihex = f.createIRI("http://www.asciitable.com/hex"), Child = iri("Child"), language = iri("language"),
        hasBirthday = iri("hasBirthday"), Unknown = iri("Unknown"), mainGraph = iri("mainGraph"),
        breakfastItem = iri("ns#breakfastItem"), originatesFrom = iri("ns#originatesFrom"), Country = iri("ns#Country"),
        officialLanguage = iri("ns#officialLanguage");
  }

  // @Test
  public void testLoadMappingWithJoinIntegration() {
    logger.info("testing JoinIntegration mapping");

    CarmlTriplesMap parentTriplesMap = CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("joinCountries.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template("http://country.example.com/{country.name}")
            .clazz(SecondExample.Country)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.officialLanguage)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("country.officialLanguage")
                .build())
            .build())
        .build();

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("joinBreakfast.xml")
            .iterator("/breakfast-menu/food")
            .referenceFormulation(Rdf.Ql.XPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template("http://food.example.com/{name}")
            .clazz(SecondExample.breakfastItem)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.originatesFrom)
                .build())
            .objectMap(CarmlRefObjectMap.newBuilder()
                .parentTriplesMap(parentTriplesMap)
                .condition(CarmlJoin.newBuilder()
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
  public void testLoadMappingWithGraphMapsPredicateObject() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("birthday")
                .build())
            .graphMap(CarmlGraphMap.newBuilder()
                .template("http://example.com/graphID/{BSN}")
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingPredObj.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithGraphMapsSubjectB() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .graphMap(CarmlGraphMap.newBuilder()
                .constant(SecondExample.mainGraph)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingSubjectB.rml.ttl");
    assertThat(result, is(expected));
  }


  // @Test
  public void testLoadMappingWithGraphMapsSubjectA() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .graphMap(CarmlGraphMap.newBuilder()
                .template("http://example.com/graphID/{BSN}")
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test15/graphMapMappingSubjectA.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithTermTypeLiteral() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("birthday")
                .datatype(XSD.DATE)
                .termType(TermType.LITERAL)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingLiteral.rml.ttl");
    assertThat(result, is(expected));;
  }

  // @Test
  public void testLoadMappingWithTermTypeIRI() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .termType(TermType.IRI)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasBirthday)
                .termType(TermType.IRI)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .constant(SecondExample.Unknown)
                .termType(TermType.IRI)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingIRI.rml.ttl");
    assertThat(result, is(expected));;
  }

  // @Test
  public void testLoadMappingWithTermTypeBlankNodeB() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .termType(TermType.BLANK_NODE)
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("birthday")
                .datatype(XSD.DATE)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeB.rml.ttl");
    assertThat(result, is(expected));;
  }

  // @Test
  public void testLoadMappingWithTermTypeBlankNodeA() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .termType(TermType.BLANK_NODE)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeA.rml.ttl");
    assertThat(result, is(expected));;
  }

  // @Test
  public void testLoadMappingWithLanguage() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("simpleTestInput.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.language)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("language")
                .language("nl")
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test5/languageMapping.rml.ttl");
    assertThat(result, is(expected));;
  }

  // @Test
  public void testLoadMappingWithSubjectConstantShortcut() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("constantShortcutMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .constant(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("code.hex")
                .build())
            .build())
        .build());

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantSubjectShortcutMapping.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithObjectConstantShortcut() {
    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("constantShortcutMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Color/{color}")
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .constant(SecondExample.asciihex)
                .build())
            .build())).build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantObjectShortcutMapping.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithSeparateMaps() {
    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("SeparateMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Color/{color}")
            .clazz(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("code.hex")
                .build())
            .build())).build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/separateMapsMappingg.rml.ttl");
    assertThat(result, is(expected));

  }

  // @Test
  public void testLoadMappingWithParentTriples() {
    TriplesMap parentTriplesMap = CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("parentTriplesTestInput.json")
            .iterator("$.colors.code")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "ColorCode/{rgba[0]},{rgba[1]},{rgba[2]}, {rgba[3]}")
            .clazz(SecondExample.RGBA)
            .build())
        .build();

    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("parentTriplesTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(SecondExample.prefix + "Color/{color}")
            .clazz(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(SecondExample.hasCode)
                .build())
            .objectMap(CarmlRefObjectMap.newBuilder()
                .parentTriplesMap(CarmlTriplesMap.newBuilder()
                    .logicalSource(CarmlLogicalSource.newBuilder()
                        .source("parentTriplesTestInput.json")
                        .iterator("$.colors.code")
                        .referenceFormulation(Rdf.Ql.JsonPath)
                        .build())
                    .subjectMap(CarmlSubjectMap.newBuilder()
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

  // @Test
  public void testLoadMappingWithJustALogicalSource() {

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("test-source.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .build());

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/logicalSourceTest.rml.ttl");

    assertThat(result, is(expected));

  }

  // @Test
  public void test() {

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.newBuilder()
        .logicalSource(CarmlLogicalSource.newBuilder()
            .source("source-a.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.newBuilder()
            .template(Example.prefix + "resource/{id}")
            .clazz(Example.MyResource)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(Example.when)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("when")
                .datatype(XSD.DATE)
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(TestRdfMapper.Example.description)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("description")
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(TestRdfMapper.Example.description)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .constant(f.createLiteral("constant description", "en"))
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.newBuilder()
            .predicateMap(CarmlPredicateMap.newBuilder()
                .constant(TestRdfMapper.Example.accuracy)
                .build())
            .objectMap(CarmlObjectMap.newBuilder()
                .reference("accuracy")
                .datatype(XSD.FLOAT)
                .build())
            .build())
        .build());

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test-a.rml.ttl");

    assertThat(result, is(expected));
  }

}
