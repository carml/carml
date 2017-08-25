package com.taxonic.rml.engine;

import java.util.Set;
import java.util.function.Function;

import com.taxonic.rml.engine.template.Template;
import com.taxonic.rml.engine.template.Template.Expression;

class GetTemplateValue implements Function<EvaluateExpression, Object> {

	private Template template;
	private Set<Expression> expressions;
	private Function<String, String> transformValue;
	private Function<Object, String> createNaturalRdfLexicalForm;

	GetTemplateValue(
		Template template,
		Set<Expression> expressions,
		Function<String, String> transformValue,
		Function<Object, String> createNaturalRdfLexicalForm
	) {
		this.template = template;
		this.expressions = expressions;
		this.transformValue = transformValue;
		this.createNaturalRdfLexicalForm = createNaturalRdfLexicalForm;
	}

	@Override
	public Object apply(EvaluateExpression evaluateExpression) {
		
		Template.Builder templateBuilder = template.newBuilder();
		expressions.forEach(e ->
			templateBuilder.bind(
				e,
				prepareValueForTemplate(
					evaluateExpression.apply(e.getValue())
				)
			)
		);
		return templateBuilder.create();
	}
	
	/**
	 * See https://www.w3.org/TR/r2rml/#from-template
	 * @param raw
	 * @return
	 */
	private String prepareValueForTemplate(Object raw) {
		if (raw == null) return "NULL";
		String value = createNaturalRdfLexicalForm.apply(raw);
		return transformValue.apply(value);
	}

}
