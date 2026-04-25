package io.carml.functions;

import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;

/** Describes and executes a single RML/FnO function. Implementations should be thread-safe. */
public interface FunctionDescriptor {

    /** Returns the IRI that identifies this function. */
    IRI getFunctionIri();

    /** Returns the parameter descriptors for this function. */
    List<ParameterDescriptor> getParameters();

    /** Returns the return value descriptors. Most functions have exactly one. */
    List<ReturnDescriptor> getReturns();

    /**
     * Executes the function with the given parameter bindings.
     *
     * <p>For single-return functions, returns the value directly. For multi-return functions,
     * returns a {@code Map<IRI, Object>} keyed by return IRI.
     */
    Object execute(Map<IRI, Object> parameterValues);
}
