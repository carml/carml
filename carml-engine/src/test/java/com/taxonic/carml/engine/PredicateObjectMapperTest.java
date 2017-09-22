package com.taxonic.carml.engine;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PredicateObjectMapperTest {

	@Test
	public void map_withPredicateMappers_callsEachPredicateMapper() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		List<PredicateMapper> predicateMappers = Arrays.asList(
				mock(PredicateMapper.class),
				mock(PredicateMapper.class),
				mock(PredicateMapper.class),
				mock(PredicateMapper.class)
		);

		Model model = new ModelBuilder().build();
		EvaluateExpression evaluator = null;


		PredicateObjectMapper testSubject = new PredicateObjectMapper(Collections.emptyList(), predicateMappers);
		testSubject.map(model, evaluator, subjectIRI, Collections.emptyList());

		predicateMappers.forEach(mapper -> verify(mapper).map(model, evaluator, subjectIRI));
	}

	@Test
	public void map_withPassedAndOwnGraphs_createsContextWithOwnAndPassedGraphs() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		PredicateMapper childMapper = mock(PredicateMapper.class);

		Model model = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		List<IRI> subjectContext = Arrays.asList(
				f.createIRI("http://subject.context/graph1"),
				f.createIRI("http://subject.context/graph2"),
				f.createIRI("http://subject.context/graph3"),
				f.createIRI("http://subject.context/graph4")
		);
		List<IRI> ownContext = Arrays.asList(
				f.createIRI("http://own.context/graph1"),
				f.createIRI("http://own.context/graph2"),
				f.createIRI("http://own.context/graph3"),
				f.createIRI("http://own.context/graph4"));
		IRI[] expectedContext =
				Stream.concat(subjectContext.stream(), ownContext.stream())
						.toArray(IRI[]::new);

		List<TermGenerator<IRI>> ownGraphGenerators = ownContext.stream()
				.map(graphIri -> {
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri)).getMock();
					return generator;
				})
				.collect(Collectors.toList());


		PredicateObjectMapper testSubject = new PredicateObjectMapper(ownGraphGenerators, Collections.singletonList(childMapper));
		testSubject.map(model, evaluator, subjectIRI, subjectContext);

		verify(childMapper).map(model, evaluator, subjectIRI, expectedContext);

	}

	@Test
	public void map_withPassedAndOwnGraphs_removesDuplicatesFromConcatenatedContext() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		PredicateMapper childMapper = mock(PredicateMapper.class);

		Model model = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		IRI subjectGraphIri = f.createIRI("http://subject.context/graph");
		IRI ownGraphIri = f.createIRI("http://own.context/graph");

		List<IRI> subjectContext = Arrays.asList(subjectGraphIri, ownGraphIri, ownGraphIri, subjectGraphIri);
		List<IRI> ownContext = Arrays.asList(ownGraphIri, subjectGraphIri, subjectGraphIri, ownGraphIri);
		IRI[] expectedContext = new IRI[] { subjectGraphIri, ownGraphIri };

		List<TermGenerator<IRI>> ownGraphGenerators = ownContext.stream()
				.map(graphIri -> {
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(Optional.of(graphIri)).getMock();
					return generator;
				})
				.collect(Collectors.toList());


		PredicateObjectMapper testSubject = new PredicateObjectMapper(ownGraphGenerators, Collections.singletonList(childMapper));
		testSubject.map(model, evaluator, subjectIRI, subjectContext);

		verify(childMapper).map(model, evaluator, subjectIRI, expectedContext);

	}
}