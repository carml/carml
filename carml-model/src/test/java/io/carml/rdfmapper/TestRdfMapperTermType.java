package io.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlDatatypeMap;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlPredicateMap;
import io.carml.model.impl.CarmlPredicateObjectMap;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.model.impl.CarmlTriplesMap;
import io.carml.vocab.Rdf;
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
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("birthday")
                .datatypeMap(CarmlDatatypeMap.builder()
                    .constant(XSD.DATE)
                    .build())
                .termType(TermType.LITERAL)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingLiteral.rml.ttl");

    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithTermTypeIri() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .termType(TermType.IRI)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasBirthday)
                .termType(TermType.IRI)
                .build())
            .objectMap(CarmlObjectMap.builder()
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
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .termType(TermType.BLANK_NODE)
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("birthday")
                .datatypeMap(CarmlDatatypeMap.builder()
                    .constant(XSD.DATE)
                    .build())
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeB.rml.ttl");
    assertThat(result, is(expected));
  }

  // @Test
  public void testLoadMappingWithTermTypeBlankNodeA() {
    Set<TriplesMap> expected = ImmutableSet.of(CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .source("simple2TestInput.json")
            .iterator("$.Child")
            .referenceFormulation(Rdf.Ql.JsonPath)
            .build())
        .subjectMap(CarmlSubjectMap.builder()
            .template(SecondExample.prefix + "Child/{first}/{last}")
            .clazz(SecondExample.Child)
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(SecondExample.hasBirthday)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .termType(TermType.BLANK_NODE)
                .build())
            .build())
        .build());
    Set<TriplesMap> result = loadRmlFromTtl("RdfMapper/test14/termTypeMappingBlankNodeA.rml.ttl");
    assertThat(result, is(expected));
  }

}
