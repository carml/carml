package io.carml.model.impl.template;

public class TemplateException extends RuntimeException {

  private static final long serialVersionUID = -851620300686694381L;

  public TemplateException(String message) {
    super(message);
  }

  public TemplateException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
