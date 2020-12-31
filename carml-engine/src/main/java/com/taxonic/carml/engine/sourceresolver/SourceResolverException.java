package com.taxonic.carml.engine.sourceresolver;

public class SourceResolverException extends RuntimeException {

  public SourceResolverException(String message) {
    super(message);
  }

  public SourceResolverException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
