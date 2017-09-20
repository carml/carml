package com.taxonic.carml.engine;

import java.util.Optional;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;

interface TermGenerator<T extends Value> extends Function<EvaluateExpression, Optional<T>> {

}
