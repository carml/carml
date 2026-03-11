package io.carml.logicalview.duckdb;

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
     * Compiles a type companion for a top-level field reference. Returns a {@code json_type()}
     * expression for JSON sources, or a {@code CAST(NULL AS VARCHAR)} expression for column-based
     * sources that have no JSON type information.
     *
     * @param reference the source reference expression
     * @param typeAlias the alias for the type companion column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileFieldTypeReference(String reference, Name typeAlias);

    /**
     * Compiles a type companion for a nested field reference within an UNNEST table.
     *
     * @param unnestAlias the alias of the UNNEST table
     * @param reference the nested field reference
     * @param typeAlias the alias for the type companion column
     * @return the jOOQ select field expression
     */
    SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias);

    /**
     * Resolves a child-side join reference into a field expression.
     *
     * @param childRef the child reference from a join condition
     * @return the jOOQ field expression for the child side of a join condition
     */
    Field<Object> resolveJoinChildReference(String childRef);
}
