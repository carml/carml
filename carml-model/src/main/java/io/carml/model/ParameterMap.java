package io.carml.model;

/**
 * An RML-FNML ParameterMap, identifying the parameter to bind a value to.
 *
 * <p>Extends {@link ExpressionMap}, inheriting {@code getConstant()} (parameter IRI),
 * {@code getReference()}, and {@code getTemplate()} for dynamic parameter selection.
 * The {@code rml:parameter} shortcut is syntactic sugar for a ParameterMap with a constant value.
 *
 * @see <a href="https://w3id.org/rml/fnml/spec">RML-FNML specification</a>
 */
public interface ParameterMap extends ExpressionMap {}
