package io.carml.logicalview.duckdb;

import java.io.Serial;

/**
 * Thrown when a DuckDB query execution fails (e.g., SQL error from the compiled view query).
 */
class DuckDbQueryException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    DuckDbQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
