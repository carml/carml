package io.carml.logicalview.duckdb;

import io.carml.jsonpath.JsonPathExpressionEvaluator;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Optional;

/**
 * An {@link ExpressionEvaluation} that evaluates JSONPath expressions against a raw JSON string
 * using the shared {@link JsonPathExpressionEvaluator}. Used to provide source-level expression
 * evaluation for DuckDB view iterations when gather map expressions need to be evaluated outside the
 * SQL query.
 *
 * <p>Delegates all evaluation logic to {@link JsonPathExpressionEvaluator}, which handles document
 * parsing, null filtering, and string conversion.
 */
final class DuckDbJsonSourceEvaluation implements ExpressionEvaluation {

    private final JsonPathExpressionEvaluator evaluator;

    /**
     * Creates a new source evaluation for the given raw JSON string.
     *
     * @param rawJson the raw JSON string from the DuckDB {@code __iter} column
     */
    DuckDbJsonSourceEvaluation(String rawJson) {
        this.evaluator = new JsonPathExpressionEvaluator(rawJson);
    }

    @Override
    public Optional<Object> apply(String expression) {
        return evaluator.apply(expression);
    }
}
