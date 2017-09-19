package com.taxonic.carml.engine;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.engine.PredicateObjectMapper;
import com.taxonic.carml.engine.SubjectMapper;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TermGeneratorCreator;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.impl.SubjectMapImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class SubjectMapperTest {

	@Test
	public void mapCallsEveryPredicateObjectMapper() throws Exception {
		PredicateObjectMapper mockMapper1 = Mockito.mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper2 = Mockito.mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper3 = Mockito.mock(PredicateObjectMapper.class);
		PredicateObjectMapper mockMapper4 = Mockito.mock(PredicateObjectMapper.class);

		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("http://foo.bar/subjectIRI");

		SubjectMap subjectMap = SubjectMapImpl.newBuilder().constant(subjectIRI).build();
		TermGeneratorCreator tgc = TermGeneratorCreator.create(null);
		TermGenerator subjectGenerator = tgc.getSubjectGenerator(subjectMap);

		Model m = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), Collections.emptySet(),
				Arrays.asList(mockMapper1, mockMapper2, mockMapper3, mockMapper4));

		s.map(m, evaluator);

		Mockito.verify(mockMapper1).map(Mockito.eq(m), Mockito.eq(evaluator), Mockito.any(), Mockito.eq(Collections.emptyList()));
		Mockito.verify(mockMapper2).map(Mockito.eq(m), Mockito.eq(evaluator), Mockito.any(), Mockito.eq(Collections.emptyList()));
		Mockito.verify(mockMapper3).map(Mockito.eq(m), Mockito.eq(evaluator), Mockito.any(), Mockito.eq(Collections.emptyList()));
		Mockito.verify(mockMapper4).map(Mockito.eq(m), Mockito.eq(evaluator), Mockito.any(), Mockito.eq(Collections.emptyList()));
	}

	@Test
	public void mapAppliesEveryClass() throws Exception {

		SimpleValueFactory f = SimpleValueFactory.getInstance();
		IRI subjectIRI = f.createIRI("https://www.none.invalid/subjectIRI");

		SubjectMap subjectMap = SubjectMapImpl.newBuilder().constant(subjectIRI).build();
		TermGeneratorCreator tgc = TermGeneratorCreator.create(null);
		TermGenerator subjectGenerator = tgc.getSubjectGenerator(subjectMap);

		HashSet<IRI> expectedClasses = new HashSet<>();
		expectedClasses.add(f.createIRI("http://www.none.invalid/foo"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/bar"));
		expectedClasses.add(f.createIRI("http://www.none.invalid/baz"));

		Model m = new ModelBuilder().build();
		EvaluateExpression evaluator = null;

		SubjectMapper s = new SubjectMapper(subjectGenerator, Collections.emptyList(), expectedClasses, Collections.emptyList());


		s.map(m, evaluator);

		expectedClasses.forEach(iri -> Assert.assertTrue(m.contains(subjectIRI, RDF.TYPE, iri)));
	}
}