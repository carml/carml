package com.taxonic.carml.engine;

public class RmlMapperException extends RuntimeException {

  private static final long serialVersionUID = 2898205613421170426L;

  public RmlMapperException(String message) {
    super(message);
  }

  public RmlMapperException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
