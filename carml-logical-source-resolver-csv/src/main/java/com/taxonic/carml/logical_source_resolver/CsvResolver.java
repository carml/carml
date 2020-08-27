package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.model.LogicalSource;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.StringReader;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvResolver implements LogicalSourceResolver<Record> {

	private static final Logger LOG = LoggerFactory.getLogger(CsvResolver.class);

	@Override
	public SourceIterator<Record> getSourceIterator() {
		return this::getItererableCsv;
	}

	private Iterable<Record> getItererableCsv(String source, LogicalSource logicalSource) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		settings.setLineSeparatorDetectionEnabled(true);
		settings.setDelimiterDetectionEnabled(true);
		settings.setReadInputOnSeparateThread(true);
		settings.setMaxCharsPerColumn(-1);
		CsvParser parser = new CsvParser(settings);

		return parser.iterateRecords(new StringReader(source));
	}

	@Override
	public ExpressionEvaluatorFactory<Record> getExpressionEvaluatorFactory() {
		return entry -> expression -> {
			logEvaluateExpression(expression, LOG);
			return Optional.ofNullable(entry.getString(expression));
		};
	}

}
