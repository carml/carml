package io.carml.jsonpath;

import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jsfr.json.compiler.JsonPathCompiler;

/**
 * Validates JSONPath expressions using JSurfer's ANTLR-based parser.
 *
 * <p>Jayway JSONPath silently accepts gibberish expressions (e.g. "Dhkef;esfkdleshfjdls;fk") as
 * property name lookups and returns null. JSurfer's compiler rejects them with a
 * {@code ParseCancellationException}, providing stricter validation.
 *
 * <p>Standard JSONPath expressions (starting with "$") are validated directly by JSurfer. Jayway
 * also accepts bare expressions without "$" prefix (e.g. "Name", "@.id", "tags[*]"). These cannot
 * be validated by JSurfer (which requires "$" prefix), so they are checked for characters that are
 * clearly not part of JSONPath syntax or member names.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonPathValidator {

    /**
     * Characters valid in bare (non-$-prefixed) JSONPath expressions: letters, digits, underscore,
     * hyphen, dot, brackets, wildcard, at, filter syntax, quotes, colon, comparison operators, and
     * space. Anything outside this set indicates invalid syntax.
     *
     * <p>This pattern is intentionally broad to avoid false positives on valid property names. It
     * only rejects clearly non-JSONPath characters such as semicolons.
     */
    private static final Pattern INVALID_BARE_NAME_CHARS = Pattern.compile("[^\\w.\\[\\]*@?()'\":=!&|><\\s-]");

    /**
     * Pattern matching valid relative JSONPath key references. Relative references (those not
     * starting with {@code $}) must be simple property names: word characters, spaces, hyphens,
     * dots (for nested access), and bracket notation ({@code ['key']}, {@code [0]}). Semicolons and
     * other non-printable or structural characters indicate malformed expressions.
     *
     * <p>This is a stricter alternative to {@link #INVALID_BARE_NAME_CHARS} that uses a whitelist
     * approach rather than a blacklist. It additionally allows commas, which are used in union
     * selectors.
     */
    private static final Pattern VALID_RELATIVE_KEY = Pattern.compile("[\\w .\\-\\[\\]'*?@(),:!<>=&|]+");

    /**
     * Validates a JSONPath expression using JSurfer's ANTLR-based parser for {@code $}-prefixed
     * expressions, and a character-based check for bare expressions.
     *
     * @param expression the JSONPath expression to validate
     * @throws JsonPathValidationException if the expression is null, empty, or has invalid syntax
     */
    public static void validate(String expression) {
        if (expression == null || expression.isEmpty()) {
            throw new JsonPathValidationException("Invalid JSONPath expression: expression must not be null or empty");
        }
        if (expression.startsWith("$")) {
            validateWithJSurfer(expression);
        } else if (containsInvalidBareNameChars(expression)) {
            throw new JsonPathValidationException("Invalid JSONPath expression: " + expression);
        }
    }

    /**
     * Validates a JSONPath expression using the stricter whitelist pattern. This rejects any
     * expression containing characters not commonly found in JSONPath property names and accessors.
     *
     * <p>This method is intended for contexts (such as DuckDB SQL compilation) where invalid
     * expressions would silently produce NULL results rather than throwing errors.
     *
     * @param expression the JSONPath expression to validate
     * @throws JsonPathValidationException if the expression is null, empty, or has invalid syntax
     */
    public static void validateStrict(String expression) {
        if (expression == null || expression.isEmpty()) {
            throw new JsonPathValidationException("Invalid JSONPath expression: expression must not be null or empty");
        }
        if (expression.startsWith("$")) {
            validateWithJSurfer(expression);
        } else if (!VALID_RELATIVE_KEY.matcher(expression).matches()) {
            throw new JsonPathValidationException(
                    "Invalid JSONPath reference expression '%s': contains invalid characters".formatted(expression));
        }
    }

    /**
     * Returns whether the given bare (non-$-prefixed) expression contains characters that are
     * clearly not valid in JSONPath syntax or member names.
     *
     * @param expression the bare expression to check (must not start with "$")
     * @return {@code true} if the expression contains invalid characters
     */
    static boolean containsInvalidBareNameChars(String expression) {
        return INVALID_BARE_NAME_CHARS.matcher(expression).find();
    }

    private static void validateWithJSurfer(String expression) {
        try {
            JsonPathCompiler.compile(expression);
        } catch (Exception e) {
            throw new JsonPathValidationException("Invalid JSONPath expression: " + expression, e);
        }
    }
}
