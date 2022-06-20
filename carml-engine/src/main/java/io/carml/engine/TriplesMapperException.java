package io.carml.engine;

public class TriplesMapperException extends RuntimeException {

  private static final long serialVersionUID = 6335891702514852884L;

  public TriplesMapperException(String message) {
    super(message);
  }

  public TriplesMapperException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
