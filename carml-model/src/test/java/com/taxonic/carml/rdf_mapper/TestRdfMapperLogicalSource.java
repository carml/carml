package com.taxonic.carml.rdf_mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Iterables;
import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.XmlSource;
import java.util.Set;

public class TestRdfMapperLogicalSource extends RmlLoader {

  // @Test
  public void mapper_givenLogicalSourceSourceWithMultipleTypes_hasTraitsMatchingTypes() {
    Set<TriplesMap> mapping = loadRmlFromTtl("RdfMapper/simple.doublyTypedCarml.rml.ttl");

    assertThat(mapping.size(), is(1));

    TriplesMap tMap = Iterables.getOnlyElement(mapping);

    Object source = tMap.getLogicalSource()
        .getSource();

    assertThat(source, instanceOf(NameableStream.class));
    assertThat(source, instanceOf(XmlSource.class));
  }

  // @Test
  public void mapper_givenLogicalSourceSourceWithSpecificFileSource_hasTraitsMatchingTypes() {
    Set<TriplesMap> mapping = loadRmlFromTtl("RdfMapper/simple.XmlFileSourceCarml.rml.ttl");

    assertThat(mapping.size(), is(1));

    TriplesMap tMap = Iterables.getOnlyElement(mapping);

    Object source = tMap.getLogicalSource()
        .getSource();

    assertThat(source, instanceOf(FileSource.class));
    assertThat(source, instanceOf(XmlSource.class));
  }

  // @Test
  public void mapper_givenLogicalSourceWithMissingSource_throwsException() {
    String expectedStart = "Error processing blank node resource";
    String expectedContains = String.format("```%n<http://none.com/#SubjectMapping> rml:logicalSource ["
        + "%n      rml:source [%n          :causedException \"<<<<<<<<<<<<<\"%n        ]%n    ] .%n```");

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> loadRmlFromTtl("RdfMapper/simple.missingSource.rml.ttl"));

    assertThat(exception.getMessage(), startsWith(expectedStart));
    assertThat(exception.getMessage(), containsString(expectedContains));
  }

}
