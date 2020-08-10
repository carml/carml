package com.taxonic.carml.logical_source_resolver;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.ExpressionEvaluatorFactory;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf.Ql;
import com.univocity.parsers.common.record.Record;
import java.util.List;
import java.util.function.Function;
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
		Iterable<Record> recordIterator = csvResolver.bindSource(LSOURCE, sourceResolver).get();
		assertThat(Iterables.size(recordIterator), is(2));
	}
	
	@Test
	public void sourceIterator_givenRandomDelimitedCsv_shoulReturnAllCorrectRecords() {
		Iterable<Record> recordIterator = csvResolver.bindSource(LSOURCE_DELIM, sourceResolver).get();
		List<Record> records = Lists.newArrayList(recordIterator);
		assertThat(records.size(), is(2));
		assertThat(records.get(0).getValues().length, is(5));
	}
	
	@Test
	public void expressionEvaluator_givenExpression_shoulReturnCorrectValue() {
		String expression = "Year";
		Iterable<Record> recordIterator = csvResolver.bindSource(LSOURCE, sourceResolver).get();
		ExpressionEvaluatorFactory<Record> evaluatorFactory = 
				csvResolver.getExpressionEvaluatorFactory();
		
		List<Record> records = Lists.newArrayList(recordIterator);
		EvaluateExpression evaluateExpression = evaluatorFactory.apply(records.get(0));
		assertThat(evaluateExpression.apply(expression).get(), is("1997"));
	}
	
}
