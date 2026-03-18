package io.carml.logicalview.duckdb;

import java.io.Serial;

/**
 * Thrown when Arrow batch transfer fails after data transfer has started. At this point the
 * JDBC ResultSet is consumed and fallback to row-by-row transfer would produce corrupt output.
 */
class ArrowTransferException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    ArrowTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
