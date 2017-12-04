package com.taxonic.carml.logical_source_resolver;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.StringReader;
import java.util.Optional;

public class CsvResolver implements LogicalSourceResolver<Record> {

	@Override
	public SourceIterator<Record> getSourceIterator() {
		return this::getItererableCsv;
	}
	
	private Iterable<Record> getItererableCsv(String source, String iteratorExpression) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		settings.setLineSeparatorDetectionEnabled(true);
		settings.setDelimiterDetectionEnabled(true);
		settings.setReadInputOnSeparateThread(true);
		CsvParser parser = new CsvParser(settings);
		
		return parser.iterateRecords(new StringReader(source));
	}

	@Override
	public ExpressionEvaluatorFactory<Record> getExpressionEvaluatorFactory() {
		return entry -> expression -> Optional.ofNullable(entry.getString(expression));
	}

}
