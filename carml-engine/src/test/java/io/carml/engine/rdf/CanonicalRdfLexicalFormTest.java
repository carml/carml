package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalRdfLexicalFormTest {

  static Stream<Arguments> rdfLexicalFormTestArgs() {
    return Stream.of(Arguments.of(.224, XSD.DECIMAL, "0.224"), Arguments.of("+001", XSD.DECIMAL, "1.0"), // different
                                                                                                         // from
                                                                                                         // https://www.w3.org/TR/r2rml/#xsd-summary
        Arguments.of(42.0, XSD.DECIMAL, "42.0"), // different from https://www.w3.org/TR/r2rml/#xsd-summary
        Arguments.of("-5.9000", XSD.DECIMAL, "-5.9"), Arguments.of("05", XSD.INTEGER, "5"),
        Arguments.of(+333, XSD.INTEGER, "333"), Arguments.of("00", XSD.INTEGER, "0"),
        Arguments.of(-5.90, XSD.DOUBLE, "-5.9E0"), Arguments.of(+0.00014770215000, XSD.DOUBLE, "1.4770215E-4"),
        Arguments.of(+01E+3, XSD.DOUBLE, "1.0E3"), Arguments.of(100.0, XSD.DOUBLE, "1.0E2"),
        Arguments.of(0, XSD.DOUBLE, "0.0E0"), Arguments.of(1, XSD.BOOLEAN, "true"),
        Arguments.of(0, XSD.BOOLEAN, "false"),
        Arguments.of(OffsetTime.parse("22:17:34.885+00:00"), XSD.TIME, "22:17:34.885Z"),
        Arguments.of(LocalTime.parse("22:17:34.000"), XSD.TIME, "22:17:34.0"),
        // different from https://www.w3.org/TR/r2rml/#xsd-summary
        Arguments.of(OffsetTime.parse("22:17:34.1+01:00"), XSD.TIME, "22:17:34.1+01:00"),
        Arguments.of(OffsetDateTime.parse("2011-08-23T22:17:00.000+00:00"), XSD.DATETIME, "2011-08-23T22:17:00.0Z")
    // different from https://www.w3.org/TR/r2rml/#xsd-summary
    );
  }

  @ParameterizedTest
  @MethodSource("rdfLexicalFormTestArgs")
  void givenValueAndDatatype_whenApply_thenReturnExpectedLexicalForm(Object value, IRI datatype, String expected) {
    // Given
    var lexicalForm = CanonicalRdfLexicalForm.get();

    // When
    String result = lexicalForm.apply(value, datatype);

    // Then
    assertThat(result, is(expected));
  }
}
