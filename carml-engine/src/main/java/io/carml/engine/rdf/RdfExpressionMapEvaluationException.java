package io.carml.engine.rdf;

public class RdfExpressionMapEvaluationException extends RuntimeException {

  public RdfExpressionMapEvaluationException(String message) {
    super(message);
  }

  public RdfExpressionMapEvaluationException(String message, Throwable cause) {
    super(message, cause);
  }
}
