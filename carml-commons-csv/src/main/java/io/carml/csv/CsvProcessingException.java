package io.carml.csv;

import java.io.Serial;

/**
 * Exception thrown when CSV/CSVW processing encounters invalid configuration, such as a
 * multi-character value where a single character is required.
 */
public class CsvProcessingException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public CsvProcessingException(String message) {
        super(message);
    }
}
