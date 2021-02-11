package com.taxonic.carml.logical_source_resolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.engine.Item;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.ExpressionEvaluatorFactory;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf.Ql;
import com.univocity.parsers.common.record.Record;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class CsvResolverTest {
	
	private static final String SOURCE = 
			"Year,Make,Model,Description,Price\r\n" + 
			"1997,Ford,E350,\"ac, abs, moon\",3000.00\r\n" + 
			"1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00" ;
	
	private static final LogicalSource LSOURCE = 
			new CarmlLogicalSource(SOURCE, null, Ql.Csv);
	
	private static final String SOURCE_DELIM = 
			"Year^Make^Model^Description^Price\r\n" + 
			"1997^Ford^E350^\"ac, abs, moon\"^3000.00\r\n" + 
			"1999^Chevy^\"Venture \"\"Extended Edition\"\"\"^\"\"^4900.00" ;
	
	private static final LogicalSource LSOURCE_DELIM = 
			new CarmlLogicalSource(SOURCE_DELIM, null, Ql.Csv);
	
	private Function<Object, String> sourceResolver = s -> s.toString();
	
	private CsvResolver csvResolver;
	
	@Before
	public void init() {
		csvResolver = new CsvResolver();
	}
	
	@Test
	public void sourceIterator_givenCsv_shoulReturnAllRecords() {	
		Stream<Item<Record>> recordStream = csvResolver.bindSource(LSOURCE, sourceResolver).get();
		assertThat(recordStream.count(), is(2L));
	}
	
	@Test
	public void sourceIterator_givenRandomDelimitedCsv_shoulReturnAllCorrectRecords() {
		Stream<Item<Record>> recordStream = csvResolver.bindSource(LSOURCE_DELIM, sourceResolver).get();
		List<Record> records = recordStream.map(Item::getItem).collect(Collectors.toList());
		assertThat(records.size(), is(2));
		assertThat(records.get(0).getValues().length, is(5));
	}
	
	@Test
	public void expressionEvaluator_givenExpression_shoulReturnCorrectValue() {
		String expression = "Year";
		Stream<Item<Record>> recordStream = csvResolver.bindSource(LSOURCE, sourceResolver).get();
		ExpressionEvaluatorFactory<Record> evaluatorFactory = 
				csvResolver.getExpressionEvaluatorFactory();
		
		List<Record> records = recordStream.map(Item::getItem).collect(Collectors.toList());
		EvaluateExpression evaluateExpression = evaluatorFactory.apply(records.get(0));
		assertThat(evaluateExpression.apply(expression).get(), is("1997"));
	}

	@Test
	public void expressionEvaluator_shouldMapLargeColumns() throws IOException {
		String csv = IOUtils.toString(CsvResolverTest.class.getResourceAsStream("large_column.csv"), StandardCharsets.UTF_8);
		LogicalSource logicalSource = new CarmlLogicalSource(csv, null, Ql.Csv);
		Stream<Item<Record>> recordStream = csvResolver.bindSource(logicalSource, sourceResolver).get();
		assertThat(recordStream.count(), is(1L));
	}
}
