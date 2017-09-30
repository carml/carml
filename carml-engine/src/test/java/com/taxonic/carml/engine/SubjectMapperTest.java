package com.taxonic.carml.engine;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;

public class SubjectMapperTest {

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	TermGenerator<Resource> subjectGenerator;

	@Test
	public void map_withPredicateObjectMappers_callsEveryPredicateObjectMapper() throws Exception {

		EvaluateExpression evaluator = null;

		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		PredicateObjectMapper mockMapper1 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper2 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper3 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper4 = mock(PredicateObjectMapper.class);
		Set<PredicateObjectMapper> predObjMappers = 
			ImmutableSet.of(mockMapper1, mockMapper2, mockMapper3, mockMapper4);

		Model model = new ModelBuilder().build();
		SubjectMapper s = 
			new SubjectMapper(subjectGenerator, ImmutableSet.of(), ImmutableSet.of(), predObjMappers);
		
		s.map(model, evaluator);

		predObjMappers.forEach(
			mapper -> 
				verify(mapper)
				.map(model, evaluator, subjectIRI, ImmutableSet.of()));
	}

	@Test
	public void map_withClasses_appliesEveryClassToGeneratedSubject() throws Exception {

		EvaluateExpression evaluator = null;
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		Set<IRI> expectedClasses = 
			ImmutableSet.of(
				f.createIRI("http://www.none.invalid/foo"),
				f.createIRI("http://www.none.invalid/bar"),
				f.createIRI("http://www.none.invalid/baz")
			);

		Model model = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, ImmutableSet.of(), expectedClasses, ImmutableSet.of());
		s.map(model, evaluator);

		expectedClasses.forEach(iri -> Assert.assertTrue(model.contains(subjectIRI, RDF.TYPE, iri)));
	}


	@Test
	public void map_withGraphsAndClasses_appliesGraphsToEveryClass() throws Exception {

		EvaluateExpression evaluator = null;

		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));


		Set<IRI> graphs = 
			ImmutableSet.of(
				f.createIRI("http://www.none.invalid/graph1"),
				f.createIRI("http://www.none.invalid/graph2"),
				f.createIRI("http://www.none.invalid/graph3"),
				f.createIRI("http://www.none.invalid/graph4")
			);
		Set<TermGenerator<IRI>> graphGenerators = 
			graphs.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri));
					return generator;
				})
				.collect(ImmutableCollectors.toImmutableSet());


		Set<IRI> expectedClasses = 
			ImmutableSet.of(
				f.createIRI("http://www.none.invalid/foo"),
				f.createIRI("http://www.none.invalid/bar"),
				f.createIRI("http://www.none.invalid/baz")
			);


		Model model = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, graphGenerators, expectedClasses, ImmutableSet.of());
		s.map(model, evaluator);

		expectedClasses.forEach(iri ->
			graphs.forEach(graph ->
					Assert.assertTrue(model.contains(subjectIRI, RDF.TYPE, iri, graph)))
		);
	}


	@Test
	public void map_withGraphsAndClasses_appliesAllGraphsToEveryPredicateObjectMapper() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		EvaluateExpression evaluator = null;

		Set<IRI> graphs = 
			ImmutableSet.of(
				f.createIRI("http://www.none.invalid/graph1"),
				f.createIRI("http://www.none.invalid/graph2"),
				f.createIRI("http://www.none.invalid/graph3"),
				f.createIRI("http://www.none.invalid/graph4")
			);
		Set<TermGenerator<IRI>> graphGenerators =
			graphs.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri));
					return generator;
				})
				.collect(ImmutableCollectors.toImmutableSet());


		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		PredicateObjectMapper mockMapper1 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper2 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper3 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper4 = mock(PredicateObjectMapper.class);
		Set<PredicateObjectMapper> predObjMappers = 
			ImmutableSet.of(mockMapper1, mockMapper2, mockMapper3, mockMapper4);

		Model m = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, graphGenerators, ImmutableSet.of(), predObjMappers);
		s.map(m, evaluator);

		predObjMappers.forEach(mapper -> verify(mapper).map(m, evaluator, subjectIRI, graphs));
	}

}