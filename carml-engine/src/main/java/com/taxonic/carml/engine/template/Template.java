package com.taxonic.carml.engine.template;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface Template {

	Set<Expression> getExpressions();

	Builder newBuilder();

	interface Builder {

		Builder bind(Expression expression, Function<Expression, Optional<Object>> templateValue);

		Optional<Object> create();

	}

	interface Expression {

		String getValue();

	}

	String toTemplateString();

}
