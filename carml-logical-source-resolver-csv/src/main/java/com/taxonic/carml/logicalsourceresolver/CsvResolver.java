package com.taxonic.carml.logicalsourceresolver;

import static com.taxonic.carml.util.LogUtil.exception;

import com.google.common.collect.Iterables;
import com.taxonic.carml.model.LogicalSource;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvResolver implements LogicalSourceResolver<Record> {

  public static CsvResolver getInstance() {
    return new CsvResolver();
  }

  @Override
  public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<Record>>> getLogicalSourceRecords(
      Set<LogicalSource> logicalSourceFilter) {
    return resolvedSource -> getCsvRecordFlux(resolvedSource, logicalSourceFilter);
  }

  private Flux<LogicalSourceRecord<Record>> getCsvRecordFlux(ResolvedSource<?> resolvedSource,
      Set<LogicalSource> logicalSources) {
    if (logicalSources.size() > 1) {
      throw new LogicalSourceResolverException(String.format(
          "Multiple logical sources found, but only one supported. Logical sources: %n%s", exception(logicalSources)));
    }

    if (logicalSources.isEmpty()) {
      throw new IllegalStateException("No logical sources registered");
    }

    var logicalSource = Iterables.getOnlyElement(logicalSources);

    var resolved = resolvedSource.getResolved();
    if (!(resolved.isPresent() && resolved.get() instanceof InputStream)) {
      throw new LogicalSourceResolverException(
          String.format("No valid input stream provided for logical source %s", logicalSource));
    }

    return getCsvRecordFlux((InputStream) resolved.get())
        .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
  }

  private Flux<Record> getCsvRecordFlux(InputStream inputStream) {
    var settings = new CsvParserSettings();
    settings.setHeaderExtractionEnabled(true);
    settings.setLineSeparatorDetectionEnabled(true);
    settings.setDelimiterDetectionEnabled(true);
    settings.setReadInputOnSeparateThread(true);
    settings.setMaxCharsPerColumn(-1);
    var parser = new CsvParser(settings);

    return Flux.fromIterable(parser.iterateRecords(inputStream));
  }

  @Override
  public LogicalSourceResolver.ExpressionEvaluationFactory<Record> getExpressionEvaluationFactory() {
    return row -> headerName -> {
      logEvaluateExpression(headerName, LOG);
      return Optional.ofNullable(row.getString(headerName));
    };
  }

}
