package io.carml.model;

/**
 * An RML-FNML ReturnMap, identifying which return value to select from a function execution.
 *
 * <p>Extends {@link ExpressionMap}, inheriting {@code getConstant()} (return value IRI),
 * {@code getReference()}, and {@code getTemplate()} for dynamic return value selection.
 * The {@code rml:return} shortcut is syntactic sugar for a ReturnMap with a constant value.
 *
 * @see <a href="https://w3id.org/rml/fnml/spec">RML-FNML specification</a>
 */
public interface ReturnMap extends ExpressionMap {}
