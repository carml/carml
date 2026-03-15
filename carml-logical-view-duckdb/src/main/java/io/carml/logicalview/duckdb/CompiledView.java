package io.carml.logicalview.duckdb;

import java.util.Set;

/**
 * The result of compiling a {@link io.carml.model.LogicalView} via {@link DuckDbViewCompiler}.
 *
 * <p>Bundles the compiled SQL query with metadata that the evaluator needs for post-query
 * validation and processing, keeping the evaluator fully format-agnostic.
 *
 * @param sql the DuckDB-compatible SQL query string
 * @param multiValuedFieldNames field names for multi-valued expression fields (e.g., JSONPath array
 *     wildcards) whose UNNEST elements should skip non-scalar type validation
 * @param nonScalarTypeValues DuckDB type strings that indicate a non-scalar value (e.g.,
 *     {@code "ARRAY"}, {@code "OBJECT"} for JSON sources). Empty for source types where all values
 *     are inherently scalar (CSV, SQL, view-on-view).
 */
record CompiledView(String sql, Set<String> multiValuedFieldNames, Set<String> nonScalarTypeValues) {}
