package com.taxonic.carml.engine;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.TemplateParser;

public class GetTemplateValueTest {
	
	@Mock
	Function<String, String> transformValue;
	
	@Mock
	Function<Object, String> createNaturalRdfLexicalForm;
	
	@Mock
	EvaluateExpression evaluateExpression;
	
	@Rule 
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	@Test
	public void getTemplateValue_givenValidInputAndFindingValue_performsAsExpected() {
		when(evaluateExpression.apply("xyz")).thenReturn(Optional.of("evaluated"));
		when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
		when(transformValue.apply("natural")).thenReturn("transformed");
		
		Template template = TemplateParser.build().parse("abc{xyz}");
		GetTemplateValue getTemplateValue = 
			new GetTemplateValue(
				template, 
				template.getExpressions(), 
				transformValue, 
				createNaturalRdfLexicalForm
			);
		Optional<Object> templateValue = getTemplateValue.apply(evaluateExpression);
		String result = unpackTemplateValue(templateValue);
		assertThat(result, is("abctransformed"));
	}
	
	private String unpackTemplateValue(Optional<Object> templateValue) {
		return templateValue.map(v -> {
			if (v instanceof List<?>) {
				List<?> list = (List<?>)v;
				assertThat(list.size(), is(1));
				return (String)(list).get(0);
			} else {
				throw new RuntimeException();
			}
		}).orElseThrow(RuntimeException::new);
	}
	
	@Test
	public void getTemplateValue_givenValidInputWithMultipleExpressions_performsAsExpected() {
		when(evaluateExpression.apply("xyz")).thenReturn(Optional.of("evaluated"));
		when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
		when(transformValue.apply("natural")).thenReturn("transformed");
		
		Template template = TemplateParser.build().parse("abc{xyz}{xyz}");
		GetTemplateValue getTemplateValue = 
			new GetTemplateValue(
				template, 
				template.getExpressions(), 
				transformValue, 
				createNaturalRdfLexicalForm
			);
		Optional<Object>templateValue = getTemplateValue.apply(evaluateExpression);
		String result = unpackTemplateValue(templateValue);
		assertThat(result, is("abctransformedtransformed"));
	}
	
	@Test
	public void getTemplateValue_givenValidInputAndNotFindingValue_returnsNoValues() {
		when(evaluateExpression.apply("xyz")).thenReturn(Optional.empty());
		when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
		when(transformValue.apply("natural")).thenReturn("transformed");
		
		Template template = TemplateParser.build().parse("abc{xyz}");
		GetTemplateValue getTemplateValue = 
			new GetTemplateValue(
				template, 
				template.getExpressions(), 
				transformValue, 
				createNaturalRdfLexicalForm
			);
		Optional<Object> templateValue = getTemplateValue.apply(evaluateExpression);
		List<?> result = templateValue.map(v -> {
			if (v instanceof List<?>) {
				List<?> list = (List<?>)v;
				return list;
			} else {
				throw new RuntimeException();
			}
		}).orElseThrow(RuntimeException::new);
		assertThat(result.size(), is(0));
	}

}
