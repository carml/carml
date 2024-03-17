package io.carml.engine.function;

import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface ExecuteFunction {

    IRI getIri();

    Object execute(Model model, Resource subject, UnaryOperator<Object> returnValueAdapter);
}
