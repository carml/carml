package com.taxonic.carml.engine.reactivedev.join;

public class MapDbStoreException extends RuntimeException {

  private static final long serialVersionUID = -1079620917655211391L;

  public MapDbStoreException(String message) {
    super(message);
  }

  public MapDbStoreException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
