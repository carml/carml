package io.carml.logicalview.duckdb;

import static io.carml.logicalview.duckdb.DuckDbSourceStrategy.ORDINAL_FIELD;
import static io.carml.logicalview.duckdb.JsonIteratorSourceStrategy.UNNEST_FIELD;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Mixed-formulation compiler for JSONPath children. The parent value is a string containing JSON
 * text, parsed using DuckDB JSON functions.
 *
 * <p>For array iterators (e.g., {@code $[*]}), the JSON array is extracted and unnested. For
 * single-value iterators (e.g., {@code $}), the JSON value is wrapped in a single-element list.
 */
final class JsonChildCompiler implements MixedFormulationCompiler {

    private final Field<?> parentValue;

    JsonChildCompiler(Field<?> parentValue) {
        this.parentValue = parentValue;
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String absoluteName) {
        if (iterator != null && iterator.contains("[*]")) {
            return table("""
                            LATERAL (SELECT unnest(json_extract({0}, {1})) AS "%s", \
                            unnest(range(len(json_extract({0}, {1})))) AS "%s")\
                            """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentValue, inline(iterator))
                    .as(quotedName(absoluteName));
        }
        var iterExpr = (iterator != null && !iterator.isBlank()) ? iterator : "$";
        return table("""
                        LATERAL (SELECT unnest(list_value(json_extract({0}, {1}))) AS "%s", \
                        unnest(list_value(0)) AS "%s")\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentValue, inline(iterExpr))
                .as(quotedName(absoluteName));
    }

    @Override
    public SelectField<?> compileNestedField(String unnestAlias, String reference, Name fieldAlias) {
        var normalized = JsonIteratorSourceStrategy.normalizeBracketNotation(reference);
        return DSL.field(
                        "json_extract_string({0}, {1})",
                        field(quotedName(unnestAlias, UNNEST_FIELD)), inline(normalized))
                .as(fieldAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldType(String unnestAlias, String reference, Name typeAlias) {
        var normalized = JsonIteratorSourceStrategy.normalizeBracketNotation(reference);
        return DSL.field("json_type({0}, {1})", field(quotedName(unnestAlias, UNNEST_FIELD)), inline(normalized))
                .as(typeAlias);
    }
}
