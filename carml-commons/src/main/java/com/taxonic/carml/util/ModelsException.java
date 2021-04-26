package com.taxonic.carml.util;

public class ModelsException extends RuntimeException {

  public ModelsException(String message) {
    super(message);
  }

  public ModelsException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
