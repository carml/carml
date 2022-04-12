package com.taxonic.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf.Ql;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class CsvResolverTest {

  private static final String SOURCE = "Year,Make,Model,Description,Price\r\n"
      + "1997,Ford,E350,\"ac, abs, moon\",3000.00\r\n" + "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

  private static final LogicalSource LSOURCE = CarmlLogicalSource.builder()
      .source(SOURCE)
      .referenceFormulation(Ql.Csv)
      .build();

  private static final String SOURCE_DELIM = "Year^Make^Model^Description^Price\r\n"
      + "1997^Ford^E350^\"ac, abs, moon\"^3000.00\r\n" + "1999^Chevy^\"Venture \"\"Extended Edition\"\"\"^\"\"^4900.00";

  private static final LogicalSource LSOURCE_DELIM = CarmlLogicalSource.builder()
      .source(SOURCE_DELIM)
      .referenceFormulation(Ql.Csv)
      .build();

  private final Function<Object, InputStream> sourceResolver =
      s -> IOUtils.toInputStream((String) s, StandardCharsets.UTF_8);

  private CsvResolver csvResolver;

  @BeforeEach
  public void init() {
    csvResolver = CsvResolver.getInstance();
  }

  @Test
  void givenCsv_whenSourceFluxApplied_givenCsv_thenReturnFluxOfAllRecords() {
    // Given
    var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
    var resolvedSource = ResolvedSource.of(SOURCE, sourceResolver.apply(SOURCE), InputStream.class);

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  void givenRandomDelimitedCsv_whenSourceFluxApplied_thenReturnFluxOfAllCorrectRecords() {
    // Given
    var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE_DELIM));
    var resolvedSource = ResolvedSource.of(SOURCE, sourceResolver.apply(SOURCE), InputStream.class);

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextMatches(logicalSourceRecord -> logicalSourceRecord.getRecord()
            .getValues().length == 5)
        .expectNextCount(1)
        .verifyComplete();
  }

  @Test
  void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
    // Given
    var expression = "Year";
    var sourceFlux = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
    var resolvedSource = ResolvedSource.of(SOURCE, sourceResolver.apply(SOURCE), InputStream.class);
    var recordFlux = sourceFlux.apply(resolvedSource);
    var record = recordFlux.blockFirst()
        .getRecord();
    var evaluationFactory = csvResolver.getExpressionEvaluationFactory();
    var evaluation = evaluationFactory.apply(record);

    // When
    var evaluationResult = evaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("1997"));
  }

  @Test
  void givenLargeColumns_whenSourceFluxApplied_thenReturnFluxWithLargeColumnRecords() throws IOException {
    var csv = IOUtils.toString(Objects.requireNonNull(CsvResolverTest.class.getResourceAsStream("large_column.csv")),
        StandardCharsets.UTF_8);
    var logicalSource = CarmlLogicalSource.builder()
        .source(csv)
        .referenceFormulation(Ql.Csv)
        .build();
    var resolvedSource = ResolvedSource.of(logicalSource.getSource(), sourceResolver.apply(csv), InputStream.class);
    var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(logicalSource));

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(1)
        .verifyComplete();
  }
}
