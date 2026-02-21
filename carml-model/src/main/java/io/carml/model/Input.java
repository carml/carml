package io.carml.model;

/**
 * An RML-FNML Input, binding a value to a parameter of a {@link FunctionExecution}.
 *
 * <p>Each Input specifies:
 * <ul>
 *   <li>a {@link ParameterMap} identifying which parameter the value binds to</li>
 *   <li>an {@link ExpressionMap} providing the input value (which can itself contain
 *       a nested {@link FunctionExecution})</li>
 * </ul>
 *
 * @see <a href="https://w3id.org/rml/fnml/spec">RML-FNML specification</a>
 */
public interface Input extends Resource {

    ParameterMap getParameterMap();

    ExpressionMap getInputValueMap();
}
