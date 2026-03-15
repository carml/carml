package io.carml.jsonpath;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Normalizes JSONPath expressions to canonical forms suitable for different evaluation engines.
 *
 * <p>DuckDB's {@code json_extract_string} does not support bracket notation with single-quoted keys
 * ({@code $['key']}). This normalizer rewrites such accessors to quoted dot notation
 * ({@code $."key"}), which DuckDB does support. RML template escape sequences ({@code \{} and
 * {@code \}}) are unescaped to their literal characters during conversion.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonPathNormalizer {

    /**
     * Matches a single bracket-notation accessor in a JSONPath expression, e.g. {@code ['key name']}.
     * The key may use single quotes and may contain RML template escape sequences ({@code \\{} and
     * {@code \\}}).
     */
    private static final Pattern BRACKET_ACCESSOR = Pattern.compile("\\['((?>[^'\\\\]|\\\\.)*+)']");

    /**
     * Converts JSONPath bracket notation accessors to quoted dot notation.
     *
     * <p>Rewrites {@code $['key']} to {@code $."key"} and unescapes RML template escape sequences
     * ({@code \{} and {@code \}}) to their literal characters. Expressions without bracket notation
     * are returned unchanged.
     *
     * @param reference the JSONPath reference, possibly containing bracket notation
     * @return the reference with bracket accessors rewritten to quoted dot notation
     */
    public static String normalizeBracketNotation(String reference) {
        Matcher matcher = BRACKET_ACCESSOR.matcher(reference);
        if (!matcher.find()) {
            return reference;
        }

        var sb = new StringBuilder();
        matcher.reset();
        while (matcher.find()) {
            var key = matcher.group(1);
            // Unescape RML template escape sequences: \{ -> { and \} -> }
            var unescaped = key.replace("\\{", "{").replace("\\}", "}");
            matcher.appendReplacement(sb, Matcher.quoteReplacement(".\"" + unescaped + "\""));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
