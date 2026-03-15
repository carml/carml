package io.carml.jsonpath;

import com.jayway.jsonpath.JsonPath;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Utility methods for analyzing JSONPath selector characteristics.
 *
 * <p>These methods help determine whether a JSONPath expression will produce a single value or
 * multiple values, which is needed by both the reactive resolver (to decide whether to return a
 * scalar or list) and the DuckDB compiler (to decide whether UNNEST is needed).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonPathSelectors {

    /**
     * Returns whether the given JSONPath expression contains a multi-valued selector such as
     * {@code [*]} (array wildcard), {@code .*} (child wildcard), or {@code ..} (recursive descent).
     *
     * <p>This check is based on simple string analysis of the expression. It does not parse the
     * JSONPath syntax tree, so it may produce false positives for unusual expressions where these
     * patterns appear inside string literals. For most practical RML mappings this is sufficient.
     *
     * @param expression the JSONPath expression to analyze
     * @return {@code true} if the expression contains a multi-valued selector pattern
     */
    public static boolean isMultiValuedSelector(String expression) {
        if (expression == null || expression.isEmpty()) {
            return false;
        }
        return expression.contains("[*]") || expression.contains(".*") || expression.contains("..");
    }

    /**
     * Returns whether the given JSONPath expression is a definite (non-wildcard) path according to
     * Jayway JSONPath. A definite path produces at most one value, while an indefinite path may
     * produce multiple values.
     *
     * <p>This delegates to {@link JsonPath#isPathDefinite(String)}, which accounts for all
     * Jayway-recognized wildcard patterns including filters, deep scans, and wildcards.
     *
     * @param expression the JSONPath expression to check
     * @return {@code true} if the expression is a definite (single-valued) path
     */
    public static boolean isDefinitePath(String expression) {
        return JsonPath.isPathDefinite(expression);
    }
}
