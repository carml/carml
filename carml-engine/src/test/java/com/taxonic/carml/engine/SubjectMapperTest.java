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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
		List<PredicateObjectMapper> predObjMappers = Arrays.asList(mockMapper1, mockMapper2, mockMapper3, mockMapper4);

		Model model = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), Collections.emptySet(), predObjMappers);
		s.map(model, evaluator);

		predObjMappers.forEach(mapper -> verify(mapper).map(model, evaluator, subjectIRI, Collections.emptyList()));
	}

	@Test
	public void map_withClasses_appliesEveryClassToGeneratedSubject() throws Exception {

		EvaluateExpression evaluator = null;
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		HashSet<IRI> expectedClasses = new HashSet<>();
		expectedClasses.add(f.createIRI("http://www.none.invalid/foo"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/bar"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/baz"));

		Model model = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), expectedClasses, Collections.emptyList());
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


		List<IRI> graphs = Arrays.asList(
				f.createIRI("http://www.none.invalid/graph1"),
				f.createIRI("http://www.none.invalid/graph2"),
				f.createIRI("http://www.none.invalid/graph3"),
				f.createIRI("http://www.none.invalid/graph4")
		);
		List<TermGenerator<IRI>> graphGenerators = graphs.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri));
					return generator;
				})
				.collect(Collectors.toList());


		HashSet<IRI> expectedClasses = new HashSet<>();
		expectedClasses.add(f.createIRI("http://www.none.invalid/foo"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/bar"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/baz"));


		Model model = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, graphGenerators, expectedClasses, Collections.emptyList());
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

		List<IRI> graphs = Arrays.asList(
				f.createIRI("http://www.none.invalid/graph1"),
				f.createIRI("http://www.none.invalid/graph2"),
				f.createIRI("http://www.none.invalid/graph3"),
				f.createIRI("http://www.none.invalid/graph4")
		);
		List<TermGenerator<IRI>> graphGenerators = graphs.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri));
					return generator;
				})
				.collect(Collectors.toList());


		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		PredicateObjectMapper mockMapper1 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper2 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper3 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper4 = mock(PredicateObjectMapper.class);
		List<PredicateObjectMapper> predObjMappers = Arrays.asList(mockMapper1, mockMapper2, mockMapper3, mockMapper4);

		Model m = new ModelBuilder().build();
		SubjectMapper s = new SubjectMapper(subjectGenerator, graphGenerators, Collections.emptySet(), predObjMappers);
		s.map(m, evaluator);

		predObjMappers.forEach(mapper -> verify(mapper).map(m, evaluator, subjectIRI, graphs));
	}

}