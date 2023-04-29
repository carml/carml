package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;

import io.carml.model.LogicalSource;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.model.impl.CarmlTriplesMap;
import io.carml.vocab.Rdf;
import java.util.List;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogUtilTest {

  private TriplesMap triplesMap;

  private LogicalSource logicalSource;

  private SubjectMap subjectMap;

  private static String ex(String name) {
    return String.format("http://example.org/%s", name);
  }

  @BeforeEach
  void beforeEach() {
    logicalSource = CarmlLogicalSource.builder()
        .id("ls")
        .referenceFormulation(Rdf.Ql.Csv)
        .build();

    subjectMap = CarmlSubjectMap.builder()
        .id(ex("sm"))
        .template(ex("{class}"))
        .clazz(OWL.CLASS)
        .build();

    triplesMap = CarmlTriplesMap.builder()
        .id(ex("tm"))
        .logicalSource(logicalSource)
        .subjectMap(subjectMap)
        .build();
  }

  @Test
  void givenSingleIriResource_whenLog_thenLogIri() {
    // Given
    var resource = triplesMap;

    // When
    var logMsg = LogUtil.log(resource);

    // Then
    assertThat(logMsg, is("resource <http://example.org/tm>"));
  }

  @Test
  void givenBlankNodeResource_whenLog_thenDescribeAncestor() {
    // Given
    var resource = logicalSource;

    // When
    var logMsg = LogUtil.log(resource);

    // Then
    assertThat(logMsg, equalToCompressingWhiteSpace("blank node resource _:ls in:" //
        + " ```" //
        + "  [] a rml:LogicalSource;" //
        + "  rml:referenceFormulation ql:CSV ." //
        + " ```"));
  }

  @Test
  void givenIriResourceCollection_whenLog_thenLogIris() {
    // Given
    var resources = List.of(triplesMap, subjectMap);

    // When
    var logMsg = LogUtil.log(resources);

    // Then
    assertThat(logMsg, equalToCompressingWhiteSpace("resource <http://example.org/tm>," //
        + " resource <http://example.org/sm>"));
  }

  @Test
  void givenIriResource_whenException_thenLogIri() {
    // Given
    var resource = triplesMap;

    // When
    var exceptionMsg = LogUtil.exception(resource);

    // Then
    assertThat(exceptionMsg, is("resource <http://example.org/tm>"));
  }

  @Test
  void givenBlankNodeResource_whenException_thenDescribeAncestor() {
    // Given
    var resource = logicalSource;

    // When
    var exceptionMsg = LogUtil.exception(resource);

    // Then
    assertThat(exceptionMsg, equalToCompressingWhiteSpace("blank node resource _:ls in:" //
        + " ```" //
        + "  [] a rml:LogicalSource;" //
        + "  :causedException \"<<<<<<<<<<<<<\";" + "  rml:referenceFormulation ql:CSV ." //
        + " ```"));
  }

  @Test
  void givenIriResourceCollection_whenException_thenLogIris() {
    // Given
    var resources = List.of(triplesMap, subjectMap);

    // When
    var exceptionMsg = LogUtil.exception(resources);

    // Then
    assertThat(exceptionMsg, equalToCompressingWhiteSpace("resource <http://example.org/tm>," //
        + " resource <http://example.org/sm>"));
  }
}
