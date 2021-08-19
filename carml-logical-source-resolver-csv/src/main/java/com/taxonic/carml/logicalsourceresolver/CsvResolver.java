package com.taxonic.carml.logicalsourceresolver;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.util.LogUtil;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.InputStream;
import java.util.Optional;
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
  public SourceFlux<Record> getSourceFlux() {
    return this::getCsvRecordFlux;
  }

  private Flux<Record> getCsvRecordFlux(Object source, LogicalSource logicalSource) {
    if (!(source instanceof InputStream)) {
      throw new LogicalSourceResolverException(
          String.format("No valid input stream provided for logical source %s", LogUtil.exception(logicalSource)));
    }

    return getCsvRecordFlux((InputStream) source);
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
