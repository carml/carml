package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.engine.Item;
import com.taxonic.carml.model.LogicalSource;
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.StringReader;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvResolver implements LogicalSourceResolver<Record> {

	private static final Logger LOG = LoggerFactory.getLogger(CsvResolver.class);

	@Override
	public SourceStream<Record> getSourceStream() {
		return this::getCsvRecordsAsStream;
	}

	private Stream<Item<Record>> getCsvRecordsAsStream(String source, LogicalSource logicalSource) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		settings.setLineSeparatorDetectionEnabled(true);
		settings.setDelimiterDetectionEnabled(true);
		settings.setReadInputOnSeparateThread(true);
		settings.setMaxCharsPerColumn(-1);
		CsvParser parser = new CsvParser(settings);

		IterableResult<Record, ParsingContext> records = parser.iterateRecords(new StringReader(source));
		ExpressionEvaluatorFactory<Record> evaluatorFactory = getExpressionEvaluatorFactory();
		return StreamSupport.stream(records.spliterator(), false)
			.map(o -> new Item<>(o, evaluatorFactory.apply(o)));
	}

	ExpressionEvaluatorFactory<Record> getExpressionEvaluatorFactory() {
		return entry -> expression -> {
			logEvaluateExpression(expression, LOG);
			return Optional.ofNullable(entry.getString(expression));
		};
	}

	@Override
	public GetStreamFromContext<Record> createGetStreamFromContext(String iterator) {
		throw new UnsupportedOperationException("not implemented - in order to use nested mappings with csv, this method must be implemented");
	}

	@Override
	public CreateContextEvaluate getCreateContextEvaluate() {
		throw new UnsupportedOperationException("not implemented - in order to use nested mappings with csv, this method must be implemented");
	}

	@Override
	public CreateSimpleTypedRepresentation getCreateSimpleTypedRepresentation() {
		return v -> v;
	}

}
