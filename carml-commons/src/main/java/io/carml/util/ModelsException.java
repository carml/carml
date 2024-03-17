package io.carml.util;

import java.io.Serial;

public class ModelsException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1321112440842409766L;

    public ModelsException(String message) {
        super(message);
    }

    public ModelsException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
