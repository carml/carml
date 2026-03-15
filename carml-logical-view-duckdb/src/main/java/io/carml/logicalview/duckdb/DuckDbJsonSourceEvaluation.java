package io.carml.logicalview.duckdb;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link ExpressionEvaluation} that evaluates JSONPath expressions against a raw JSON string
 * using Jayway JSONPath. Used to provide source-level expression evaluation for DuckDB view
 * iterations when gather map expressions need to be evaluated outside the SQL query.
 *
 * <p>The JSON document is parsed eagerly in the constructor and reused for subsequent evaluations
 * within the same iteration. Multi-valued expressions (e.g., {@code $.values.*}) return a {@link List}
 * of string values, matching the contract expected by
 * {@link ExpressionEvaluation#extractValues(Object)}.
 */
final class DuckDbJsonSourceEvaluation implements ExpressionEvaluation {

    private static final Configuration JSONPATH_CONF = Configuration.builder()
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.ALWAYS_RETURN_LIST)
            .build();

    private final Object parsedDocument;

    /**
     * Creates a new source evaluation for the given raw JSON string.
     *
     * @param rawJson the raw JSON string from the DuckDB {@code __iter} column
     */
    DuckDbJsonSourceEvaluation(String rawJson) {
        this.parsedDocument =
                Configuration.defaultConfiguration().jsonProvider().parse(rawJson);
    }

    @Override
    public Optional<Object> apply(String expression) {
        List<?> resultList;
        try {
            resultList = JsonPath.using(JSONPATH_CONF).parse(parsedDocument).read(expression, List.class);
        } catch (PathNotFoundException e) {
            return Optional.empty();
        }

        if (resultList == null || resultList.isEmpty()) {
            return Optional.empty();
        }

        // Filter out null values and convert to strings
        var values = resultList.stream()
                .filter(Objects::nonNull)
                .map(DuckDbJsonSourceEvaluation::toStringValue)
                .filter(Objects::nonNull)
                .toList();

        if (values.isEmpty()) {
            return Optional.empty();
        }

        if (values.size() == 1) {
            return Optional.of(values.get(0));
        }

        return Optional.of(values);
    }

    private static String toStringValue(Object value) {
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof Collection<?> || value instanceof Map<?, ?>) {
            // Nested arrays/objects: skip — not meaningful as RDF literal values
            return null;
        }
        return String.valueOf(value);
    }
}
