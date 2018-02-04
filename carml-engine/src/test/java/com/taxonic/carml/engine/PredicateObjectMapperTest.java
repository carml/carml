package com.taxonic.carml.engine;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;

public class PredicateObjectMapperTest {

	@Test
	public void map_withPredicateMappers_callsEachPredicateMapper() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		Set<PredicateMapper> predicateMappers = ImmutableSet.of(
				mock(PredicateMapper.class),
				mock(PredicateMapper.class),
				mock(PredicateMapper.class),
				mock(PredicateMapper.class)
		);

		Model model = new ModelBuilder().build();
		EvaluateExpression evaluator = null;


		PredicateObjectMapper testSubject = new PredicateObjectMapper(ImmutableSet.of(), predicateMappers);
		testSubject.map(model, evaluator, subjectIRI, ImmutableSet.of());

		predicateMappers.forEach(mapper -> verify(mapper).map(model, evaluator, subjectIRI));
	}

	@Test
	public void map_withPassedAndOwnGraphs_createsContextWithOwnAndPassedGraphs() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		PredicateMapper childMapper = mock(PredicateMapper.class);

		Model model = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		Set<IRI> subjectContext = ImmutableSet.of(
				f.createIRI("http://subject.context/graph1"),
				f.createIRI("http://subject.context/graph2"),
				f.createIRI("http://subject.context/graph3"),
				f.createIRI("http://subject.context/graph4")
		);
		Set<IRI> ownContext = ImmutableSet.of(
				f.createIRI("http://own.context/graph1"),
				f.createIRI("http://own.context/graph2"),
				f.createIRI("http://own.context/graph3"),
				f.createIRI("http://own.context/graph4"));
		IRI[] expectedContext =
				Stream.concat(subjectContext.stream(), ownContext.stream())
						.toArray(IRI[]::new);

		Set<TermGenerator<IRI>> ownGraphGenerators = ownContext.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(ImmutableList.of(graphIri)).getMock();
					return generator;
				})
				.collect(ImmutableCollectors.toImmutableSet());


		PredicateObjectMapper testSubject = new PredicateObjectMapper(ownGraphGenerators, ImmutableSet.of(childMapper));
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

		Set<IRI> subjectContext = ImmutableSet.of(subjectGraphIri, ownGraphIri, ownGraphIri, subjectGraphIri);
		Set<IRI> ownContext = ImmutableSet.of(ownGraphIri, subjectGraphIri, subjectGraphIri, ownGraphIri);
		IRI[] expectedContext = new IRI[] { subjectGraphIri, ownGraphIri };

		Set<TermGenerator<IRI>> ownGraphGenerators = ownContext.stream()
				.map(graphIri -> {
					@SuppressWarnings("unchecked")
					TermGenerator<IRI> generator = (TermGenerator<IRI>) mock(TermGenerator.class);
					when(generator.apply(evaluator)).thenReturn(ImmutableList.of(graphIri)).getMock();
					return generator;
				})
				.collect(ImmutableCollectors.toImmutableSet());


		PredicateObjectMapper testSubject = new PredicateObjectMapper(ownGraphGenerators, ImmutableSet.of(childMapper));
		testSubject.map(model, evaluator, subjectIRI, subjectContext);

		verify(childMapper).map(model, evaluator, subjectIRI, expectedContext);

	}
}