package com.taxonic.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
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

public class TestRdfMapperTermType extends RmlLoader {

  static final SimpleValueFactory f = SimpleValueFactory.getInstance();

  static class SecondExample {

    static IRI iri(String suffix) {
      return f.createIRI(prefix + suffix);
    }

    static final String prefix = "http://example.com/";

    static final IRI Unknown = iri("Unknown");

    static final IRI Child = iri("Child");

    static final IRI hasBirthday = iri("hasBirthday");
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

    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithTermTypeIri() {
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
    assertThat(result, is(expected));
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
    assertThat(result, is(expected));
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
    assertThat(result, is(expected));
  }

}
