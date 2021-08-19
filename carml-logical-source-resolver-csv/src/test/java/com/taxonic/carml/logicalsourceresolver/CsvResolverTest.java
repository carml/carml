package com.taxonic.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.util.ReactiveInputStreams;
import com.taxonic.carml.vocab.Rdf.Ql;
import com.univocity.parsers.common.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
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

  private final Function<Object, InputStream> sourceResolver = s -> {
    try {
      return ReactiveInputStreams.inputStreamFrom(
          ReactiveInputStreams.fluxInputStream(IOUtils.toInputStream((String) s, StandardCharsets.UTF_8)));
    } catch (IOException ioException) {
      throw new RuntimeException(ioException);
    }
  };

  private CsvResolver csvResolver;

  @BeforeEach
  public void init() {
    csvResolver = CsvResolver.getInstance();
  }

  @Test
  void givenCsv_whenSourceFluxApplied_givenCsv_thenReturnFluxOfAllRecords() throws InterruptedException {
    // Given
    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();

    // When
    Flux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(SOURCE), LSOURCE);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  void givenRandomDelimitedCsv_whenSourceFluxApplied_thenReturnFluxOfAllCorrectRecords() {
    // Given
    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();

    // When
    Flux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(SOURCE), LSOURCE_DELIM);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextMatches(record -> record.getValues().length == 5)
        .expectNextCount(1)
        .verifyComplete();
  }

  @Test
  void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
    // Given
    String expression = "Year";
    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();
    Flux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(SOURCE), LSOURCE);
    Record record = recordFlux.blockFirst();
    LogicalSourceResolver.ExpressionEvaluationFactory<Record> evaluationFactory =
        csvResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation evaluation = evaluationFactory.apply(record);

    // When
    Optional<Object> evaluationResult = evaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("1997"));
  }

  @Test
  void givenLargeColumns_whenSourceFluxApplied_thenReturnFluxWithLargeColumnRecords() throws IOException {
    String csv = IOUtils.toString(Objects.requireNonNull(CsvResolverTest.class.getResourceAsStream("large_column.csv")),
        StandardCharsets.UTF_8);
    LogicalSource logicalSource = CarmlLogicalSource.builder()
        .source(csv)
        .referenceFormulation(Ql.Csv)
        .build();
    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();

    // When
    Flux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(csv), logicalSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(1)
        .verifyComplete();
  }
}
