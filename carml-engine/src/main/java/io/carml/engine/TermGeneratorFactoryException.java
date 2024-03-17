package io.carml.engine;

import java.io.Serial;

public class TermGeneratorFactoryException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -5655378944971053150L;

    public TermGeneratorFactoryException(String message) {
        super(message);
    }

    public TermGeneratorFactoryException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
