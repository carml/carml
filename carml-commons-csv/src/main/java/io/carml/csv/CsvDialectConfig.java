package io.carml.csv;

import java.util.Optional;

/**
 * Resolved CSVW dialect configuration with all properties extracted from a {@code CsvwDialect} and
 * defaults applied.
 *
 * <p>This record provides a backend-agnostic representation of CSV dialect settings. Consumers
 * (e.g., FastCSV-based reactive resolver, DuckDB SQL compiler) use these resolved values to
 * configure their respective parsing backends.
 *
 * @param delimiter the field delimiter character, if specified
 * @param quoteChar the quote character, if specified
 * @param commentPrefix the comment prefix character, if specified
 * @param encoding the character encoding name (e.g., "UTF-8"), if specified
 * @param skipRows the number of initial rows to skip (0 means none)
 * @param useDoubleQuoteEscaping whether double-quote escaping is enabled; {@code false} indicates
 *     backslash escaping should be used instead
 * @param trim whether leading and trailing whitespace should be trimmed from field values
 */
public record CsvDialectConfig(
        Optional<Character> delimiter,
        Optional<Character> quoteChar,
        Optional<Character> commentPrefix,
        Optional<String> encoding,
        int skipRows,
        boolean useDoubleQuoteEscaping,
        boolean trim) {

    /** A default configuration with no dialect overrides applied. */
    public static final CsvDialectConfig DEFAULTS = new CsvDialectConfig(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 0, true, false);
}
