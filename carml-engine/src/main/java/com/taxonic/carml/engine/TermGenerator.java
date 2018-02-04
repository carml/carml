package com.taxonic.carml.engine;

import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;

interface TermGenerator<T extends Value> extends Function<EvaluateExpression, List<T>> {

}
