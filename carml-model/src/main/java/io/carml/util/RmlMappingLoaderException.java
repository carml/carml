package io.carml.util;

import java.io.Serial;

public class RmlMappingLoaderException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 422963239354718978L;

    public RmlMappingLoaderException(String message) {
        super(message);
    }

    public RmlMappingLoaderException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
