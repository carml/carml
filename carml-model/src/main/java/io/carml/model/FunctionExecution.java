package io.carml.model;

import java.util.Set;

/**
 * An RML-FNML FunctionExecution, representing the execution of a function with bound inputs.
 *
 * <p>A FunctionExecution specifies:
 * <ul>
 *   <li>a {@link FunctionMap} identifying which function to call</li>
 *   <li>a set of {@link Input} bindings providing parameter values</li>
 * </ul>
 *
 * <p>An {@link ExpressionMap} can reference a FunctionExecution via
 * {@link ExpressionMap#getFunctionExecution()}, optionally combined with a {@link ReturnMap}
 * to select which return value to use.
 *
 * @see <a href="https://w3id.org/rml/fnml/spec">RML-FNML specification</a>
 */
public interface FunctionExecution extends Resource {

    FunctionMap getFunctionMap();

    Set<Input> getInputs();
}
