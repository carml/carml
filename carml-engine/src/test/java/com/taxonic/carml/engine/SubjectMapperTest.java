package com.taxonic.carml.engine;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

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
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubjectMapperTest {

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	TermGenerator<Resource> subjectGenerator;

	@Test
	public void map_withPredicateObjectMappers_callsEveryPredicateObjectMapper() throws Exception {
		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		Model m = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		PredicateObjectMapper mockMapper1 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper2 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper3 = mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper4 = mock(PredicateObjectMapper.class);


		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), Collections.emptySet(),
				Arrays.asList(mockMapper1, mockMapper2, mockMapper3, mockMapper4));

		s.map(m, evaluator);

		Mockito.verify(mockMapper1).map(m, evaluator, subjectIRI, Collections.emptyList());
		Mockito.verify(mockMapper2).map(m, evaluator, subjectIRI, Collections.emptyList());
		Mockito.verify(mockMapper3).map(m, evaluator, subjectIRI, Collections.emptyList());
		Mockito.verify(mockMapper4).map(m, evaluator, subjectIRI, Collections.emptyList());
	}

	@Test
	public void map_withClasses_appliesEveryClassToGeneratedSubject() throws Exception {

		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		Model m = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		when(subjectGenerator.apply(evaluator))
				.thenReturn(Optional.of(subjectIRI));

		HashSet<IRI> expectedClasses = new HashSet<>();
		expectedClasses.add(f.createIRI("http://www.none.invalid/foo"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/bar"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/baz"));

		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), expectedClasses, Collections.emptyList());


		s.map(m, evaluator);

		expectedClasses.forEach(iri -> Assert.assertTrue(m.contains(subjectIRI, RDF.TYPE, iri)));
	}
}