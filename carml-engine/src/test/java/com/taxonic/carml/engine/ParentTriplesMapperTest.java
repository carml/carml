package com.taxonic.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ParentTriplesMapperTest {
	
	@Mock
	private TermGenerator<Resource> subjectGenerator;
	
	@Mock
	private Supplier<Iterable<Object>> getIterator;
	
	@Mock
	private LogicalSourceResolver.ExpressionEvaluatorFactory<Object> expressionEvaluatorFactory;
	
	@Rule 
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	Set<Pair<String, Object>> joinValues =
			ImmutableSet.of(
				Pair.of("country.name", "Belgium")
			);

	Set<Pair<String, Object>> multiJoinValues =
			ImmutableSet.of(
				Pair.of("country.name", Arrays.asList("Belgium", null, "Netherlands"))
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
		when(subjectGenerator.apply(evaluate)).thenReturn(ImmutableList.of(SKOS.CONCEPT));
		ParentTriplesMapper<Object> mapper = new ParentTriplesMapper<>(subjectGenerator, getIterator, expressionEvaluatorFactory);
		Set<Resource> resources = mapper.map(joinValues);
		assertThat(resources.size(), is(1));
		assertThat(SKOS.CONCEPT, is(in(resources)));
	}

	@Test
	public void parentTriplesMapper_givenMultiJoinWithNullValues_ShouldStillResolveJoin() {
		when(getIterator.get()).thenReturn(ImmutableList.of(entry));
		when(expressionEvaluatorFactory.apply(entry)).thenReturn(evaluate);
		when(subjectGenerator.apply(evaluate)).thenReturn(ImmutableList.of(SKOS.CONCEPT));
		ParentTriplesMapper<Object> mapper = new ParentTriplesMapper<>(subjectGenerator, getIterator, expressionEvaluatorFactory);
		Set<Resource> resources = mapper.map(multiJoinValues);
		assertThat(resources.size(), is(1));
		assertThat(SKOS.CONCEPT, is(in(resources)));
	}

	// TODO
	public void parentTriplesMapper_notGivenJoinConditions() {
		
	}
}
