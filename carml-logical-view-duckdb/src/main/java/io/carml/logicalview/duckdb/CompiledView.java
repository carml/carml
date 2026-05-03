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
 * @param syntheticScalarOrdinalFields field names of top-level scalar reference fields whose
 *     {@code .#} ordinal column was suppressed from the SQL projection because it is structurally
 *     always {@code 0}. The evaluator synthesises {@code <fieldName>.#} = {@code 0L} entries for
 *     these fields when materialising each iteration row, preserving the downstream contract that
 *     {@code iteration.getValue("<fieldName>.#")} returns {@code Optional.of(0L)} and
 *     {@code iteration.getNaturalDatatype("<fieldName>.#")} returns {@code XSD.INTEGER}.
 * @param syntheticScalarTypeFields field names of top-level scalar reference fields whose
 *     {@code .__type} type companion column was suppressed from the SQL projection because the
 *     source strategy reported that the column would carry no information (e.g. CSV's constant
 *     {@code CAST(NULL AS VARCHAR)}). Used by recursive parent-view compilation in
 *     {@code DuckDbViewCompiler.compileJoinDescriptor} to synthesise an equivalent {@code NULL}
 *     projection on the join path instead of selecting a non-existent column.
 */
record CompiledView(
        String sql,
        Set<String> multiValuedFieldNames,
        Set<String> nonScalarTypeValues,
        Set<String> syntheticScalarOrdinalFields,
        Set<String> syntheticScalarTypeFields) {}
