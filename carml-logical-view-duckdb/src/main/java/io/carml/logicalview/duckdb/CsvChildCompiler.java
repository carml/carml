package io.carml.logicalview.duckdb;

import static io.carml.logicalview.duckdb.DuckDbSourceStrategy.ORDINAL_FIELD;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Mixed-formulation compiler for CSV children. The parent value is a string containing CSV text
 * (with header row and data rows separated by newlines).
 *
 * <p>The CSV text is split by newlines, the first line (header) is skipped for data rows, and each
 * data row is unnested. Nested fields are extracted by dynamically computing the column position
 * from the header row.
 *
 * <p><b>Limitation:</b> CSV parsing uses {@code string_split} by comma — no RFC 4180 support for
 * quoted fields, escaped delimiters, or CRLF line endings. Fields containing commas or newlines will
 * produce incorrect results.
 */
final class CsvChildCompiler implements MixedFormulationCompiler {

    static final String ROW_VAL_FIELD = "row_val";

    @SuppressWarnings("UnstableApiUsage")
    private static final DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    private final String renderedParentValue;

    CsvChildCompiler(Field<?> parentValue) {
        this.renderedParentValue = CTX.render(parentValue);
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String absoluteName) {
        var unnestSql = """
                LATERAL (SELECT "%1$s", (row_number() OVER () - 1) AS "%2$s" \
                FROM (SELECT unnest(string_split(%3$s, chr(10))[2:]) AS "%1$s") sub \
                WHERE length(trim("%1$s")) > 0)\
                """.formatted(ROW_VAL_FIELD, ORDINAL_FIELD, renderedParentValue);
        return table(unnestSql).as(quotedName(absoluteName));
    }

    @Override
    public SelectField<?> compileNestedField(String unnestAlias, String reference, Name fieldAlias) {
        var renderedRef = CTX.render(inline(reference));
        var headerExpr = "list_extract(string_split(%s, chr(10)), 1)".formatted(renderedParentValue);
        var positionExpr = "list_position(string_split(%s, ','), %s)".formatted(headerExpr, renderedRef);
        var rowValRef = CTX.render(field(quotedName(unnestAlias, ROW_VAL_FIELD)));
        var extractSql = "trim(list_extract(string_split(%s, ','), %s))".formatted(rowValRef, positionExpr);
        return DSL.field(extractSql).as(fieldAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldType(String unnestAlias, String reference, Name typeAlias) {
        return DSL.castNull(String.class).as(typeAlias);
    }
}
