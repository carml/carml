package io.carml.csv;

import io.carml.model.source.csvw.CsvwDialect;
import java.util.Optional;
import lombok.experimental.UtilityClass;

/**
 * Extracts a structured {@link CsvDialectConfig} from a {@link CsvwDialect} model object.
 *
 * <p>This processor validates and resolves all CSVW dialect properties into a backend-agnostic
 * configuration record. Properties that require single-character values (delimiter, quoteChar,
 * commentPrefix) are validated and converted accordingly.
 */
@UtilityClass
public class CsvwDialectProcessor {

    /**
     * Extracts a {@link CsvDialectConfig} from the given CSVW dialect.
     *
     * @param dialect the CSVW dialect to process; must not be {@code null}
     * @return a fully resolved dialect configuration
     * @throws CsvProcessingException if a character property contains more than one character
     */
    public static CsvDialectConfig process(CsvwDialect dialect) {
        return new CsvDialectConfig(
                toChar(dialect.getDelimiter(), "CSVW delimiter"),
                toChar(dialect.getQuoteChar(), "CSVW quote character"),
                toChar(dialect.getCommentPrefix(), "CSVW comment prefix"),
                Optional.ofNullable(dialect.getEncoding()),
                dialect.getSkipRows(),
                resolveDoubleQuoteEscaping(dialect.getDoubleQuote()),
                dialect.trim());
    }

    /**
     * Converts a string property to a single character, validating that it is at most one character
     * long.
     *
     * @param value the string value to convert
     * @param propertyName the property name used in error messages
     * @return an {@link Optional} containing the character, or empty if the value is {@code null}
     *     or empty
     * @throws CsvProcessingException if the value contains more than one character
     */
    static Optional<Character> toChar(String value, String propertyName) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        if (value.length() > 1) {
            throw new CsvProcessingException(
                    "%s must be a single character, but was '%s'".formatted(propertyName, value));
        }
        return Optional.of(value.charAt(0));
    }

    private static boolean resolveDoubleQuoteEscaping(String doubleQuote) {
        return !"false".equalsIgnoreCase(doubleQuote);
        // Default: double-quote escaping enabled (CSVW spec default)
    }
}
