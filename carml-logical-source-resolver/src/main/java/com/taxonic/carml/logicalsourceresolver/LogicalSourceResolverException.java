package com.taxonic.carml.logical_source_resolver;

public class LogicalSourceResolverException extends RuntimeException {

  public LogicalSourceResolverException(String message) {
    super(message);
  }

  public LogicalSourceResolverException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
