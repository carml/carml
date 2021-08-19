package com.taxonic.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlDatatypeMap;
import com.taxonic.carml.model.impl.CarmlLanguageMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.model.impl.CarmlObjectMap;
import com.taxonic.carml.model.impl.CarmlPredicateMap;
import com.taxonic.carml.model.impl.CarmlPredicateObjectMap;
import com.taxonic.carml.model.impl.CarmlSubjectMap;
import com.taxonic.carml.model.impl.CarmlTriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class TestRdfMapperBasic extends RmlLoader {

  private static final SimpleValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  static class Example {

    static IRI iri(String suffix) {
      return VALUE_FACTORY.createIRI(prefix + suffix);
    }

    static final String prefix = "http://data.example.com/";

    static final IRI MyResource = iri("def/MyResource");

    static final IRI when = iri("def/when");

    static final IRI description = iri("def/description");

    static final IRI accuracy = iri("def/accuracy");

  }

  static class SecondExample {

    static IRI iri(String suffix) {
      return VALUE_FACTORY.createIRI(prefix + suffix);
    }

    static final String prefix = "http://example.com/";

    static final IRI RGBA = iri("RGBA");

    static final IRI Color = iri("Color");

    static final IRI hasCode = iri("hasCode");

    static final IRI hasHex = iri("hasHex");

    static final IRI asciihex = VALUE_FACTORY.createIRI("http://www.asciitable.com/hex");

    static final IRI Child = iri("Child");

    static final IRI language = iri("language");

    static final IRI hasBirthday = iri("hasBirthday");
  }

  // @Test
  public void testLoadMappingWithLanguage() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("simpleTestInput.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.language)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("language")
                .languageMap(CarmlLanguageMap.builder()
                    .constant(VALUE_FACTORY.createLiteral("nl"))
                    .build())
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test5/languageMapping.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithSubjectConstantShortcut() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("constantShortcutMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .constant(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("code.hex")
                .build())
            .build())
        .build());

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantSubjectShortcutMapping.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithObjectConstantShortcut() {
    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("constantShortcutMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Color/{color}")
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .constant(SecondExample.asciihex)
                .build())
            .build())).build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test1/constantObjectShortcutMapping.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithSeparateMaps() {
    Set<TriplesMap> expected = ImmutableSet.of((CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("SeparateMappingTestInput.json")
            .iterator("$.colors")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Color/{color}")
            .clazz(SecondExample.Color)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasHex)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("code.hex")
                .build())
            .build())).build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test10/separateMapsMappingg.rml.ttl");
    assertThat(result, is(expected));

  }

  // @Test
  public void testLoadMappingWithJustALogicalSource() {

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
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

    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("source-a.json")
            .iterator("$")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(Example.prefix + "resource/{id}")
            .clazz(Example.MyResource)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(Example.when)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("when")
                .datatypeMap(CarmlDatatypeMap.builder()
                    .constant(XSD.DATE)
                    .build())
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(TestRdfMapperBasic.Example.description)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("description")
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(TestRdfMapperBasic.Example.description)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .constant(VALUE_FACTORY.createLiteral("constant description", "en"))
                .build())
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(TestRdfMapperBasic.Example.accuracy)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("accuracy")
                .datatypeMap(CarmlDatatypeMap.builder()
                    .constant(XSD.FLOAT)
                    .build())
                .build())
            .build())
        .build());

    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test-a.rml.ttl");

    assertThat(result, is(expected));
  }

}
