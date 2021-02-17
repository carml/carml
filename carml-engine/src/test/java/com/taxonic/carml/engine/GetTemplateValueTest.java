package com.taxonic.carml.engine;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

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

	@Mock
	UnaryOperator<Object> createSimpleTypedRepresentation;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Test
	public void getTemplateValue_givenValidInputAndFindingValue_performsAsExpected() {
		when(createSimpleTypedRepresentation.apply("evaluatedRaw")).thenReturn("evaluated");
		when(evaluateExpression.apply("xyz")).thenReturn(Optional.of("evaluatedRaw"));
		when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
		when(transformValue.apply("natural")).thenReturn("transformed");

		Template template = TemplateParser.build().parse("abc{xyz}");
		GetTemplateValue getTemplateValue =
			new GetTemplateValue(
				template,
				template.getExpressions(),
				transformValue,
				createNaturalRdfLexicalForm,
				createSimpleTypedRepresentation
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
				createNaturalRdfLexicalForm,
				v -> v
			);
		Optional<Object>templateValue = getTemplateValue.apply(evaluateExpression);
		String result = unpackTemplateValue(templateValue);
		assertThat(result, is("abctransformedtransformed"));
	}

	@Test
	public void getTemplateValue_givenValidInputAndNotFindingValue_returnsNoValues() {
		when(evaluateExpression.apply("xyz")).thenReturn(Optional.empty());

		Template template = TemplateParser.build().parse("abc{xyz}");
		GetTemplateValue getTemplateValue =
			new GetTemplateValue(
				template,
				template.getExpressions(),
				transformValue,
				createNaturalRdfLexicalForm,
				v -> v
			);
		Optional<Object> templateValue = getTemplateValue.apply(evaluateExpression);
		assertThat(templateValue, is(Optional.empty()));
	}

}
