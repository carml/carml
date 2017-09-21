package com.taxonic.carml.engine;

import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.impl.ObjectMapImpl;

import org.eclipse.rdf4j.model.ValueFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Function;

import static org.junit.Assert.*;

public class TermGeneratorCreatorTest {


	@Test
	public void getGenerator_withReferenceAndTemplate_throwsRuntimeException() throws Exception {

		TermGeneratorCreator tgc = new TermGeneratorCreator(null, "foo", null, TemplateParser.build(), null);

		RuntimeException exception = null;
		try{
			tgc.getObjectGenerator(new ObjectMapImpl("foo.bar", null, "foo{foo.bar}", TermType.LITERAL, null, null, null, null));

			Assert.assertFalse("Should have thrown exception", true);
		} catch(RuntimeException e) {
			exception = e;
		}

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.getMessage().startsWith("2 generators were created for map"));
	}
}