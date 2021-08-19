package com.taxonic.carml.rdfmapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.model.impl.CarmlObjectMap;
import com.taxonic.carml.model.impl.CarmlPredicateMap;
import com.taxonic.carml.model.impl.CarmlPredicateObjectMap;
import com.taxonic.carml.model.impl.CarmlTriplesMap;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;

public class TestFunctionModelMapping {

  private RmlMappingLoader loader = RmlMappingLoader.build();

  static final ValueFactory f = SimpleValueFactory.getInstance();

  static class Ex {

    static final String prefix = "http://example.com/";

    static IRI iri(String localName) {
      return f.createIRI(prefix, localName);
    }

    static final IRI toBoolFunction = iri("toBoolFunction");

    static final IRI isPresentBool = iri("isPresentBool");

  }

  // @Test
  public void test() {

    TriplesMap functionMap = CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(Rdf.Fno.executes)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .constant(Ex.toBoolFunction)
                .build())
            .build())
        .build();

    CarmlTriplesMap main = CarmlTriplesMap.builder()
        .logicalSource(CarmlLogicalSource.builder()
            .build())
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .constant(Ex.isPresentBool)
                .build())
            .objectMap(CarmlObjectMap.builder()
                .functionValue(functionMap)
                .build())
            .build())
        .build();

    Set<TriplesMap> expected = ImmutableSet.of(main, functionMap);

    Set<TriplesMap> result = loader.load(RDFFormat.TURTLE, "RmlMapper/test11/toBoolMapping2.fnml.ttl");

    assertThat(result, is(expected));

  }

}
