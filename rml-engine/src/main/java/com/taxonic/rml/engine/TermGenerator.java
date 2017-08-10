package com.taxonic.rml.engine;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;

interface TermGenerator<T extends Value> extends Function<EvaluateExpression, T> {

}
