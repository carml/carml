package io.carml.logicalsourceresolver.sourceresolver;

import java.io.Serial;

public class SourceResolverException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -5720641872220161835L;

    public SourceResolverException(String message) {
        super(message);
    }

    public SourceResolverException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
