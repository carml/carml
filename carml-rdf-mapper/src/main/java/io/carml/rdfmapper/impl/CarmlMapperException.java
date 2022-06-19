package io.carml.rdfmapper.impl;

public class CarmlMapperException extends RuntimeException {

  private static final long serialVersionUID = -4394105191666988073L;

  public CarmlMapperException(String message) {
    super(message);
  }

  public CarmlMapperException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
