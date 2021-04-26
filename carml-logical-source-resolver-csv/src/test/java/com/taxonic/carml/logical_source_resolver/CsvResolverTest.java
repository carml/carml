package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.util.ReactiveInputStreams;
import com.taxonic.carml.vocab.Rdf.Ql;
import com.univocity.parsers.common.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

public class CsvResolverTest {

  private static final String SOURCE = "Year,Make,Model,Description,Price\r\n"
      + "1997,Ford,E350,\"ac, abs, moon\",3000.00\r\n" + "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

  private static final LogicalSource LSOURCE = new CarmlLogicalSource(SOURCE, null, Ql.Csv);

  private static final String SOURCE_DELIM = "Year^Make^Model^Description^Price\r\n"
      + "1997^Ford^E350^\"ac, abs, moon\"^3000.00\r\n" + "1999^Chevy^\"Venture \"\"Extended Edition\"\"\"^\"\"^4900.00";

  private static final LogicalSource LSOURCE_DELIM = new CarmlLogicalSource(SOURCE_DELIM, null, Ql.Csv);

  private Function<Object, InputStream> sourceResolver = s -> {
    try {
      return ReactiveInputStreams
          .inputStreamFrom(ReactiveInputStreams.fluxInputStream(IOUtils.toInputStream((String) s, StandardCharsets.UTF_8)));
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
  public void givenCSV_whensourceIterator_givenCsv_shouldReturnAllRecords() throws InterruptedException {

    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();

    ParallelFlux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(SOURCE), LSOURCE)
        .publish()
        .autoConnect(2)
        .parallel()
        .runOn(Schedulers.boundedElastic());

    ParallelFlux<String> stringFlux1 =
        recordFlux.flatMap(record -> Flux.fromIterable(Arrays.asList(record.getValues(0, 1, 2, 3, 4))));

    ParallelFlux<String> stringFlux2 =
        recordFlux.flatMap(record -> Flux.fromIterable(Arrays.asList(record.getValues(0, 1, 2, 3, 4))
            .stream()
            .map(str -> str + " foo")
            .collect(Collectors.toList())));

    ConnectableFlux<String> merged = Flux.merge(stringFlux1, stringFlux2)
        .publish();

    merged.subscribe(System.out::println);

    merged.connect();
    // ConnectableFlux<Tuple2<String, String>> zipped = Flux.zip(stringFlux1, stringFlux2).publish();
    //
    // zipped.subscribe(tuple -> tuple.forEach(System.out::println));
    //
    // zipped.connect();
    // new CountDownLatch(1).await(100, TimeUnit.SECONDS);


    // Iterable<Record> recordIterator = csvResolver.getSourceFlux(LSOURCE, sourceResolver)
    // .get();
    // assertThat(Iterables.size(recordIterator), is(2));
  }

  @Test
  public void sourceIterator_givenCsv_shouldReturnAllRecords2() throws InterruptedException {

    LogicalSourceResolver.SourceFlux<Record> sourceFlux = csvResolver.getSourceFlux();

    Flux<Record> recordFlux = sourceFlux.apply(sourceResolver.apply(SOURCE), LSOURCE)
        .publish()
        .autoConnect(2);

    Flux<String> stringFlux1 = recordFlux.publishOn(Schedulers.boundedElastic())
        .flatMap(record -> Flux.fromIterable(Arrays.asList(record.getValues(0, 1, 2, 3, 4))));

    Flux<String> stringFlux2 = recordFlux.publishOn(Schedulers.boundedElastic())
        .flatMap(record -> Flux.fromIterable(Arrays.asList(record.getValues(0, 1, 2, 3, 4))
            .stream()
            .map(str -> str + " foo")
            .collect(Collectors.toList())));

    ConnectableFlux<String> merged = Flux.merge(stringFlux1, stringFlux2)
        .publish();

    merged.subscribe(System.out::println);

    merged.connect();

    // Iterable<Record> recordIterator = csvResolver.getSourceFlux(LSOURCE, sourceResolver)
    // .get();
    // assertThat(Iterables.size(recordIterator), is(2));
  }

  // @Test
  // public void sourceIterator_givenRandomDelimitedCsv_shoulReturnAllCorrectRecords() {
  // Iterable<Record> recordIterator = csvResolver.bindSource(LSOURCE_DELIM, sourceResolver)
  // .get();
  // List<Record> records = Lists.newArrayList(recordIterator);
  // assertThat(records.size(), is(2));
  // assertThat(records.get(0)
  // .getValues().length, is(5));
  // }
  //
  // @Test
  // public void expressionEvaluator_givenExpression_shoulReturnCorrectValue() {
  // String expression = "Year";
  // Iterable<Record> recordIterator = csvResolver.bindSource(LSOURCE, sourceResolver)
  // .get();
  // ExpressionEvaluatorFactory<Record> evaluatorFactory =
  // csvResolver.getExpressionEvaluatorFactory();
  //
  // List<Record> records = Lists.newArrayList(recordIterator);
  // EvaluateExpression evaluateExpression = evaluatorFactory.apply(records.get(0));
  // assertThat(evaluateExpression.apply(expression)
  // .get(), is("1997"));
  // }
  //
  // @Test
  // public void expressionEvaluator_shouldMapLargeColumns() throws IOException {
  // String csv =
  // IOUtils.toString(CsvResolverTest.class.getResourceAsStream("large_column.csv"),
  // StandardCharsets.UTF_8);
  // LogicalSource logicalSource = new CarmlLogicalSource(csv, null, Ql.Csv);
  // Iterable<Record> recordIterator = csvResolver.bindSource(logicalSource, sourceResolver)
  // .get();
  // assertThat(Iterables.size(recordIterator), is(1));
  // }
}
