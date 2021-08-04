package com.taxonic.carml.engine.template;

public class TemplateException extends RuntimeException {

  public TemplateException(String message) {
    super(message);
  }

  public TemplateException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
