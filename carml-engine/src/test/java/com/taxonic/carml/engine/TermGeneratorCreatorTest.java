package com.taxonic.carml.engine;

import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.impl.CarmlObjectMap;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

public class TermGeneratorCreatorTest {


	@Test
	public void getGenerator_withReferenceAndTemplate_throwsRuntimeException() throws Exception {

		TermGeneratorCreator tgc = new TermGeneratorCreator(null, "foo", null, TemplateParser.build(), null);

		RuntimeException exception = null;
		try{
			tgc.getObjectGenerator(new CarmlObjectMap("foo.bar", null, "foo{foo.bar}", TermType.LITERAL, null, null, null, null));

			Assert.assertFalse("Should have thrown exception", true);
		} catch(RuntimeException e) {
			exception = e;
		}

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.getMessage().startsWith("2 generators were created for map"));
	}

	@Test
	public void IriTermGenerator_withRelativeIRI_usesBaseIRI() throws Exception {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "http://base.iri";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String relativeIriPart = "/relativeIriPortion";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));
		List<Value> result = generator.apply(evaluator);

		Assert.assertTrue(!result.isEmpty());
		Assert.assertTrue(result.get(0) instanceof IRI);
		Assert.assertEquals(result.get(0), f.createIRI(baseIri + relativeIriPart));
	}

	@Test
	public void IriTermGenerator_withAbsoluteIRI_usesBaseIRI() throws Exception {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "http://base.iri";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String absoluteIri = "http://foo.bar/absoluteIri";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(absoluteIri));
		List<Value> result = generator.apply(evaluator);

		Assert.assertTrue(!result.isEmpty());
		Assert.assertTrue(result.get(0) instanceof IRI);
		Assert.assertEquals(result.get(0), f.createIRI(absoluteIri));
	}

	@Test
	public void IriTermGenerator_withFaultyIRI_throwsException() throws Exception {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "?";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String relativeIriPart = "/relativeIriPortion";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));

		RuntimeException exception = null;
		try {
			generator.apply(evaluator);
			Assert.assertTrue("This code should be unreachable", false);
		} catch (RuntimeException e) {
			exception = e;
		}

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.getMessage().startsWith("data error: could not generate a valid iri"));
	}

}