package com.taxonic.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlGraphMap;
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

public class TestRdfMapperGraphMaps extends RmlLoader {

  static final SimpleValueFactory f = SimpleValueFactory.getInstance();

  static class SecondExample {

    static IRI iri(String suffix) {
      return f.createIRI(prefix + suffix);
    }

    static final String prefix = "http://example.com/";

    static final IRI Child = iri("Child");

    static final IRI language = iri("language");

    static final IRI hasBirthday = iri("hasBirthday");

    static final IRI mainGraph = iri("mainGraph");
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
    assertThat(expected, is(result));
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
    assertThat(expected, is(result));
  }

}
