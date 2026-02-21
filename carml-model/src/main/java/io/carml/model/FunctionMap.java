package io.carml.model;

/**
 * An RML-FNML FunctionMap, identifying the function to execute.
 *
 * <p>Extends {@link ExpressionMap}, inheriting {@code getConstant()} (function IRI),
 * {@code getReference()}, and {@code getTemplate()} for dynamic function selection.
 * The {@code rml:function} shortcut is syntactic sugar for a FunctionMap with a constant value.
 *
 * @see <a href="https://w3id.org/rml/fnml/spec">RML-FNML specification</a>
 */
public interface FunctionMap extends ExpressionMap {}
