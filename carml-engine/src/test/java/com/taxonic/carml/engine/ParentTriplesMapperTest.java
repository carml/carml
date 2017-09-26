package com.taxonic.carml.engine;

import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.taxonic.carml.resolvers.LogicalSourceResolver;

public class ParentTriplesMapperTest {
	
	@Mock
	private TermGenerator<Resource> subjectGenerator;
	
	@Mock
	private Supplier<Iterable<Object>> getIterator;
	
	@Mock
	private LogicalSourceResolver.ExpressionEvaluatorFactory expressionEvaluatorFactory;
	
	@Rule 
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	Map<String, Object> joinValues = 
		ImmutableMap.of(
			"country.name", "Belgium"
		);
	
	String entry = 
			"{\r\n" + 
			"\"food\": {\r\n" + 
			"	\"name\": \"Belgian Waffles\",\r\n" + 
			"	\"countryOfOrigin\": \"Belgium\"\r\n" + 
			"	}\r\n" + 
			"}";
	
	EvaluateExpression evaluate = s -> Optional.of("Belgium");

	@Test
	public void parentTriplesMapper_givenJoinConditions() {
		when(getIterator.get()).thenReturn(ImmutableList.of(entry));
		when(expressionEvaluatorFactory.apply(entry)).thenReturn(evaluate);
		when(subjectGenerator.apply(evaluate)).thenReturn(Optional.of(SKOS.CONCEPT));
		ParentTriplesMapper mapper = new ParentTriplesMapper(subjectGenerator, getIterator, expressionEvaluatorFactory);
		Set<Resource> resources = mapper.map(joinValues);
		assertThat(resources.size(), is(1));
		assertThat(SKOS.CONCEPT, isIn(resources));
	}
	
	public void parentTriplesMapper_notGivenJoinConditions() {
		
	}
}
