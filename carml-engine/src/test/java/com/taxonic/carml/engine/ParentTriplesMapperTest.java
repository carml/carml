package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

public class ParentTriplesMapperTest {
	
	@Mock
	private TermGenerator<Resource> subjectGenerator;
	
	@Mock
	private Supplier<Stream<Item<Object>>> getStream;
	
	@Mock
	EvaluateExpression evaluate;

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

	IRI belgianWaffles = SimpleValueFactory.getInstance().createIRI("http://example.org/food/BelgianWaffles");

	String countryNameField = "country.name";

	ParentTriplesMapper<Object> mapper;

	@Before
	public void setup() {
		mapper = new ParentTriplesMapper<>(subjectGenerator, getStream);
		when(getStream.get()).thenReturn(Stream.of(new Item<>(entry, evaluate)));
		when(subjectGenerator.apply(evaluate)).thenReturn(ImmutableList.of(belgianWaffles));
	}

	@Test
	public void parentTriplesMapper_givenJoinConditions() {
		when(evaluate.apply(countryNameField)).thenReturn(Optional.of("Belgium"));
		Set<Resource> resources = mapper.map(joinValues);
		assertThat(resources.size(), is(1));
		assertThat(belgianWaffles, is(in(resources)));
	}

	@Test
	public void parentTriplesMapper_givenMultiJoinWithNullValues_ShouldStillResolveJoin() {
		when(evaluate.apply(countryNameField)).thenReturn(Optional.of("Belgium"));
		Set<Resource> resources = mapper.map(multiJoinValues);
		assertThat(resources.size(), is(1));
		assertThat(belgianWaffles, is(in(resources)));
	}

	@Test
	public void parentTriplesMapper_givenMultipleParentValues_succeeds() {
		when(evaluate.apply(countryNameField)).thenReturn(Optional.of(ImmutableSet.of("Belgium", "Germany")));
		Set<Resource> resources = mapper.map(multiJoinValues);
		assertThat(resources.size(), is(1));
		assertThat(belgianWaffles, is(in(resources)));
	}

	@Test
	public void parentTriplesMapper_givenMultipleParentValues_fails() {
		when(evaluate.apply(countryNameField)).thenReturn(Optional.of(ImmutableSet.of("Norway", "Germany")));
		Set<Resource> resources = mapper.map(multiJoinValues);
		assertThat(resources.size(), is(0));
	}

	// TODO
	public void parentTriplesMapper_notGivenJoinConditions() {
		
	}
}
