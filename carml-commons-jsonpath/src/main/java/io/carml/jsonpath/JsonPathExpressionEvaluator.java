package io.carml.jsonpath;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Evaluates JSONPath expressions against parsed JSON documents using Jayway JSONPath.
 *
 * <p>The evaluator parses the JSON document eagerly and reuses the parsed form for subsequent
 * evaluations. Multi-valued expressions (e.g., {@code $.values[*]}) return a {@link List} of string
 * values. Single-valued expressions return a single string. Nested objects and arrays are skipped
 * (they are not meaningful as RDF literal values).
 *
 * <p>This evaluator uses Jayway's default JSON provider (which returns Java Maps and Lists). For
 * Jackson {@code JsonNode}-based evaluation (needed by the reactive resolver for iterable field
 * support), use the resolver's own evaluation directly.
 */
public final class JsonPathExpressionEvaluator {

    private static final Configuration JSONPATH_CONF = Configuration.builder()
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.ALWAYS_RETURN_LIST)
            .build();

    private final Object parsedDocument;

    /**
     * Creates a new evaluator for the given raw JSON string.
     *
     * @param rawJson the raw JSON string to evaluate expressions against
     */
    public JsonPathExpressionEvaluator(String rawJson) {
        this.parsedDocument =
                Configuration.defaultConfiguration().jsonProvider().parse(rawJson);
    }

    /**
     * Evaluates a JSONPath expression against the parsed document.
     *
     * <p>Returns {@link Optional#empty()} if the expression does not match any path in the
     * document, or if all matched values are null. For single-valued results, returns an
     * {@code Optional<String>}. For multi-valued results, returns an {@code Optional<List<String>>}.
     *
     * <p>Nested objects and arrays are skipped -- they are not meaningful as RDF literal values.
     *
     * @param expression the JSONPath expression to evaluate
     * @return the evaluation result, or empty if no values matched
     */
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
                .map(JsonPathExpressionEvaluator::toStringValue)
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
            // Nested arrays/objects: skip -- not meaningful as RDF literal values
            return null;
        }
        return String.valueOf(value);
    }
}
