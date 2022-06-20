package io.carml.engine.sourceresolver;

public class SourceResolverException extends RuntimeException {

  private static final long serialVersionUID = -5720641872220161835L;

  public SourceResolverException(String message) {
    super(message);
  }

  public SourceResolverException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
