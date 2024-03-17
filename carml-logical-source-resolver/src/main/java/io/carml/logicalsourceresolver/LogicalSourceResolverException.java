package io.carml.logicalsourceresolver;

import java.io.Serial;

public class LogicalSourceResolverException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -4426763997103947764L;

    public LogicalSourceResolverException(String message) {
        super(message);
    }

    public LogicalSourceResolverException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
