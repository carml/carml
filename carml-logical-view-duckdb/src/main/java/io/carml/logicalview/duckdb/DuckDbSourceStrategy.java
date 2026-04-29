package io.carml.logicalview.duckdb;

import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import java.util.Optional;
import java.util.Set;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;

/**
 * Encapsulates source-specific field access and unnesting logic for DuckDB SQL compilation.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@link ColumnSourceStrategy} — direct column references from the CTE</li>
 *   <li>{@link JsonIteratorSourceStrategy} — field extraction from a JSON column using
 *       DuckDB JSON functions</li>
 * </ul>
 */
@SuppressWarnings("java:S1452")
sealed interface DuckDbSourceStrategy permits ColumnSourceStrategy, JsonIteratorSourceStrategy {

    /**
     * Column name for the 0-based ordinal produced by UNNEST tables. Each LATERAL subquery emits
     * this column alongside the unnested value so that iterable field indices ({@code field.#})
     * reset per parent row.
     */
    String ORDINAL_FIELD = "__ord";

    /**
     * Suffix for type companion columns. Each reference-based expression field has a companion
     * column with this suffix that carries the JSON type of the value (e.g., "BIGINT", "DOUBLE",
     * "BOOLEAN", "VARCHAR"). Used by the evaluator to infer natural RDF datatypes.
     */
    String TYPE_SUFFIX = ".__type";

    /**
     * Determines whether the given reference expression evaluates to multiple values. Multi-valued
     * references (e.g., JSONPath array wildcards like {@code $.items[*]}) produce multiple results
     * per source row and require UNNEST expansion rather than simple field extraction.
     *
     * @param reference the reference expression to check
     * @return {@code true} if the reference is multi-valued
     */
    boolean isMultiValuedReference(String reference);

    /**
     * Compiles a top-level field reference into a SELECT expression.
     *
     * @param reference the source reference expression
     * @param fieldAlias the alias for the result column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileFieldReference(String reference, Name fieldAlias);

    /**
     * Compiles a template expression segment reference into a field expression.
     *
     * @param segmentValue the template segment reference expression
     * @return the jOOQ field expression
     */
    Field<?> compileTemplateReference(String segmentValue);

    /**
     * Compiles a nested field reference within an UNNEST table.
     *
     * @param unnestAlias the alias of the UNNEST table
     * @param reference the nested field reference
     * @param fieldAlias the alias for the result column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileNestedFieldReference(String unnestAlias, String reference, Name fieldAlias);

    /**
     * Compiles an UNNEST table expression for an iterable field.
     *
     * @param iterator the iterator expression
     * @param parentAlias the alias of the parent table
     * @param isRootLevel {@code true} if the iterable is at the root level (not nested)
     * @param absoluteName the absolute name used as the UNNEST table alias
     * @return the jOOQ table expression
     */
    Table<?> compileUnnestTable(String iterator, String parentAlias, boolean isRootLevel, String absoluteName);

    /**
     * Compiles a type companion for a top-level field reference. The result depends on the strategy:
     * {@code json_type()} for JSON sources, {@code typeof()} for SQL sources, {@code CAST(NULL AS
     * VARCHAR)} for CSV sources, or a column projection for view-on-view sources.
     *
     * @param reference the source reference expression
     * @param typeAlias the alias for the type companion column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileFieldTypeReference(String reference, Name typeAlias);

    /**
     * Compiles a type companion for a nested field reference within an UNNEST table. Follows the
     * same strategy-dependent behavior as {@link #compileFieldTypeReference}.
     *
     * @param unnestAlias the alias of the UNNEST table
     * @param reference the nested field reference
     * @param typeAlias the alias for the type companion column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias);

    /**
     * Resolves a child-side join {@link ExpressionMap} into a field expression. The expression map
     * may use any of the RML 2 expression-map shapes: a reference, a template, or a constant.
     *
     * @param childMap the child expression map from a join condition
     * @return the jOOQ field expression for the child side of a join condition
     * @throws UnsupportedOperationException if the expression map shape cannot be compiled to SQL
     *     (e.g., a function-valued expression map)
     */
    Field<?> resolveJoinChildExpression(ExpressionMap childMap);

    /**
     * Returns the set of DuckDB type strings that indicate a non-scalar value for this source type.
     * Per the RML spec, a reference that resolves to a non-scalar value (e.g., an array or object)
     * should produce an error rather than a stringified representation.
     *
     * <p>For JSON sources, this returns {@code {"ARRAY", "OBJECT"}} because DuckDB's
     * {@code json_extract_string} stringifies such values instead of returning NULL. For column-based
     * sources (CSV, SQL, view-on-view), all values are inherently scalar, so this returns an empty
     * set.
     *
     * @return an unmodifiable set of DuckDB type strings considered non-scalar
     */
    Set<String> nonScalarTypeValues();

    /**
     * Returns the column name used for source-level expression evaluation, or empty if the source
     * does not support it.
     *
     * <p>For JSON iterator sources, this returns the {@code __iter} column that carries the raw JSON
     * for each iteration row, enabling expression evaluation at mapping time. For column-based
     * sources (CSV, SQL, view-on-view), source-level evaluation is not supported and this returns
     * empty.
     *
     * @return the source evaluation column name, or empty if not supported
     */
    Optional<String> sourceEvaluationColumn();

    /**
     * Compiles a multi-valued {@link ExpressionField} into an {@link UnnestDescriptor}. Multi-valued
     * fields are those whose reference evaluates to multiple values (e.g., {@code $.items[*]} in
     * JSONPath), requiring row expansion via UNNEST.
     *
     * <p>The field's reference is used as an iterator expression for UNNEST, and the unnested value
     * is extracted using the strategy's nested field reference compilation. When the reference
     * contains filter expressions (e.g., {@code $.items[?(@.active==true)]}), the UNNEST table is
     * wrapped with a WHERE clause that applies the filter conditions, with ordinals recomputed after
     * filtering.
     *
     * @param field the multi-valued expression field to compile
     * @param cteAlias the alias of the CTE containing the source data
     * @return the unnest descriptor containing the table expression and nested select fields
     * @throws UnsupportedOperationException if the source type does not support multi-valued fields
     */
    UnnestDescriptor compileMultiValuedUnnestDescriptor(ExpressionField field, String cteAlias);
}
