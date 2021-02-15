package com.taxonic.carml.engine;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.impl.CarmlObjectMap;

public class TermGeneratorCreatorTest {


	@Test
	public void getGenerator_withReferenceAndTemplate_throwsRuntimeException() {

		TermGeneratorCreator tgc = new TermGeneratorCreator(null, "foo", null, TemplateParser.build(), null, null);

		RuntimeException exception = assertThrows(RuntimeException.class, () ->
				tgc.getObjectGenerator(new CarmlObjectMap("foo.bar", null, "foo{foo.bar}",
						TermType.LITERAL, null, null, null, null)));
		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.getMessage().startsWith("2 value generators were created for term map"));
	}

	@Test
	public void IriTermGenerator_withRelativeIRI_usesBaseIRI() {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "http://base.iri";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null, null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String relativeIriPart = "/relativeIriPortion";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));
		List<Value> result = generator.apply(evaluator);

		Assert.assertFalse(result.isEmpty());
		Assert.assertTrue(result.get(0) instanceof IRI);
		Assert.assertEquals(result.get(0), f.createIRI(baseIri + relativeIriPart));
	}

	@Test
	public void IriTermGenerator_withAbsoluteIRI_usesBaseIRI() {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "http://base.iri";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null, null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String absoluteIri = "http://foo.bar/absoluteIri";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(absoluteIri));
		List<Value> result = generator.apply(evaluator);

		Assert.assertFalse(result.isEmpty());
		Assert.assertTrue(result.get(0) instanceof IRI);
		Assert.assertEquals(result.get(0), f.createIRI(absoluteIri));
	}

	@Test
	public void IriTermGenerator_withFaultyIRI_throwsException() {

		ValueFactory f = SimpleValueFactory.getInstance();

		String baseIri = "?";
		TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(), null, null);

		String ref = "ref";
		TermGenerator<Value> generator = tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null, null));

		String relativeIriPart = "/relativeIriPortion";
		EvaluateExpression evaluator = Mockito.mock(EvaluateExpression.class);
		when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));

		RuntimeException exception = null;
		try {
			generator.apply(evaluator);
			Assert.fail("This code should be unreachable");
		} catch (RuntimeException e) {
			exception = e;
		}

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.getMessage().startsWith("Could not generate a valid iri"));
	}

}