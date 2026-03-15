package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import io.carml.jsonpath.JsonPathNormalizer;
import io.carml.jsonpath.JsonPathValidationException;
import io.carml.jsonpath.JsonPathValidator;
import io.carml.model.ExpressionField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Source strategy for JSON sources with iterators.
 *
 * <p>Field values are extracted from a designated JSON column using {@code json_extract_string}.
 * UNNEST tables use {@code json_extract} to navigate JSON arrays and objects.
 *
 * <p>The column name for the JSON iterator rows is provided at construction time by the compiler,
 * which creates the CTE that produces this column.
 */
final class JsonIteratorSourceStrategy implements DuckDbSourceStrategy {

    /**
     * JSON types that indicate a non-scalar value. Per the RML spec, a reference that resolves to
     * an array or object should produce no term. DuckDB's {@code json_extract_string} stringifies
     * such values instead of returning NULL, so they must be detected via the {@code __type}
     * companion columns and rejected.
     */
    private static final Set<String> NON_SCALAR_JSON_TYPES = Set.of("ARRAY", "OBJECT");

    private static final String JSON_EXTRACT_STRING = "json_extract_string({0}, {1})";

    private static final DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    static final String UNNEST_FIELD = "unnest";

    private final String cteAlias;

    private final String iterColumn;

    private final Map<String, String> fieldNameToRefMap;

    private JsonIteratorSourceStrategy(String cteAlias, String iterColumn, Map<String, String> fieldNameToRefMap) {
        this.cteAlias = cteAlias;
        this.iterColumn = iterColumn;
        this.fieldNameToRefMap = fieldNameToRefMap;
    }

    static JsonIteratorSourceStrategy create(Set<io.carml.model.Field> viewFields, String cteAlias, String iterColumn) {
        var fieldNameToRefMap = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> f.getReference() != null)
                .collect(Collectors.toUnmodifiableMap(ExpressionField::getFieldName, ExpressionField::getReference));
        return new JsonIteratorSourceStrategy(cteAlias, iterColumn, fieldNameToRefMap);
    }

    /**
     * Delegates to {@link JsonPathNormalizer#normalizeBracketNotation(String)}.
     */
    static String normalizeBracketNotation(String reference) {
        return JsonPathNormalizer.normalizeBracketNotation(reference);
    }

    @Override
    public boolean isMultiValuedReference(String reference) {
        if (reference == null) {
            return false;
        }
        try {
            var parsed = JsonPathAnalyzer.analyze(reference);
            return parsed.basePath().contains("[*]") || parsed.basePath().contains(".*") || parsed.hasDeepScan();
        } catch (IllegalArgumentException e) {
            // Invalid JSONPath syntax cannot be multi-valued. The error will surface
            // later during field compilation with a descriptive message.
            return false;
        }
    }

    @Override
    public SelectField<?> compileFieldReference(String reference, Name fieldAlias) {
        validateJsonPathSyntax(reference);
        var normalized = normalizeBracketNotation(reference);
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(normalized))
                .as(fieldAlias);
    }

    @Override
    public Field<?> compileTemplateReference(String segmentValue) {
        validateJsonPathSyntax(segmentValue);
        var normalized = normalizeBracketNotation(segmentValue);
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(normalized));
    }

    @Override
    public SelectField<?> compileNestedFieldReference(String unnestAlias, String reference, Name fieldAlias) {
        // DuckDB's FROM-clause unnest wraps results in STRUCT(unnest JSON),
        // so access the inner JSON value via the "unnest" field.
        validateJsonPathSyntax(reference);
        var normalized = normalizeBracketNotation(reference);
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(unnestAlias, UNNEST_FIELD)), inline(normalized))
                .as(fieldAlias);
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String parentAlias, boolean isRootLevel, String absoluteName) {
        var parsed = JsonPathAnalyzer.analyze(iterator);
        var basePath = parsed.basePath();

        // Use the raw iterator for the SQL expression so DuckDB applies filter expressions natively.
        // Only fall back to the normalized basePath when there are no filters.
        var sqlPath = parsed.filters().isEmpty() ? basePath : iterator;

        // Root level: extract from the iterator column (plain JSON from SELECT-list unnest)
        // Nested level: extract from "parent"."unnest" (STRUCT-wrapped JSON from FROM-clause unnest)
        var parentRef =
                isRootLevel ? field(quotedName(cteAlias, iterColumn)) : field(quotedName(parentAlias, UNNEST_FIELD));

        if (isArrayResult(parsed)) {
            return compileArrayUnnest(parsed, sqlPath, parentRef, absoluteName);
        }

        // Single value: json_extract returns JSON, wrap in list_value for unnest.
        return table("""
                        LATERAL (SELECT unnest(list_value(json_extract({0}, {1}))) AS "%s", \
                        unnest(list_value(0)) AS "%s")\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static boolean isArrayResult(JsonPathAnalyzer.ParsedJsonPath parsed) {
        return parsed.basePath().contains("[*]") || parsed.basePath().contains(".*") || parsed.hasDeepScan();
    }

    private Table<?> compileArrayUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String sqlPath, Field<?> parentRef, String absoluteName) {
        // Check for slice selector with actual bounds ([:] falls through to normal unnest)
        var sliceTable = compileSliceUnnest(parsed, sqlPath, parentRef, absoluteName);
        if (sliceTable != null) {
            return sliceTable;
        }

        // Check for union selectors
        var unionTable = compileUnionUnnest(parsed, sqlPath, parentRef, absoluteName);
        if (unionTable != null) {
            return unionTable;
        }

        // Standard array unnest with parallel ordinal generation
        return table("""
                        LATERAL (SELECT unnest(json_extract({0}, {1})) AS "%s", \
                        unnest(range(len(json_extract({0}, {1})))) AS "%s")\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static Table<?> compileSliceUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String sqlPath, Field<?> parentRef, String absoluteName) {
        if (parsed.slices().isEmpty()) {
            return null;
        }

        // Slice selector: unnest full array, filter by ordinal range, recompute ordinals.
        // JSONPath slices are 0-based start-inclusive, end-exclusive.
        // [:] (both null, no step) is equivalent to [*], so return null to fall through.
        var slice = parsed.slices().get(parsed.slices().size() - 1);
        if (slice.start() == null && slice.end() == null && slice.step() == null) {
            return null;
        }

        var whereClause = buildSliceWhereClause(slice);

        return table("""
                        LATERAL (SELECT "%1$s", (row_number() over() - 1) AS "%2$s" \
                        FROM (SELECT unnest(json_extract({0}, {1})) AS "%1$s", \
                        unnest(range(len(json_extract({0}, {1})))) AS "%2$s") \
                        WHERE %3$s)\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD, whereClause), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static String buildSliceWhereClause(JsonPathAnalyzer.SliceSelector slice) {
        var conditions = new ArrayList<String>();
        var ord = "\"" + ORDINAL_FIELD + "\"";

        if (slice.start() != null) {
            conditions.add("%s >= %s".formatted(ord, boundExpr(slice.start())));
        }
        if (slice.end() != null) {
            conditions.add("%s < %s".formatted(ord, boundExpr(slice.end())));
        }
        if (slice.step() != null) {
            var startExpr = slice.start() != null ? boundExpr(slice.start()) : "0";
            conditions.add("(%s - %s) %% %d = 0".formatted(ord, startExpr, slice.step()));
        }

        return String.join(" AND ", conditions);
    }

    /** Returns a SQL expression for a slice bound, using array length for negative values. */
    private static String boundExpr(int bound) {
        return bound >= 0 ? String.valueOf(bound) : "len(json_extract({0}, {1})) + %d".formatted(bound);
    }

    private static Table<?> compileUnionUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String sqlPath, Field<?> parentRef, String absoluteName) {
        if (parsed.unions().isEmpty()) {
            return null;
        }

        var union = parsed.unions().get(parsed.unions().size() - 1);

        if (union instanceof JsonPathAnalyzer.IndexUnion indexUnion) {
            return compileIndexUnionUnnest(indexUnion, sqlPath, parentRef, absoluteName);
        }
        if (union instanceof JsonPathAnalyzer.NameUnion nameUnion) {
            return compileNameUnionUnnest(nameUnion, parsed.basePath(), parentRef, absoluteName);
        }
        throw new UnsupportedOperationException("Unsupported union selector type: %s".formatted(union.getClass()));
    }

    private static Table<?> compileIndexUnionUnnest(
            JsonPathAnalyzer.IndexUnion indexUnion, String sqlPath, Field<?> parentRef, String absoluteName) {
        var inList = indexUnion.indices().stream().map(String::valueOf).collect(Collectors.joining(", "));

        return table("""
                        LATERAL (SELECT "%1$s", (row_number() over() - 1) AS "%2$s" \
                        FROM (SELECT unnest(json_extract({0}, {1})) AS "%1$s", \
                        unnest(range(len(json_extract({0}, {1})))) AS "%2$s") \
                        WHERE "%2$s" IN (%3$s))\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD, inList), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static Table<?> compileNameUnionUnnest(
            JsonPathAnalyzer.NameUnion nameUnion, String basePath, Field<?> parentRef, String absoluteName) {
        // The basePath ends with [*] (e.g., "$.details[*]"). Strip the [*] to get the parent
        // object path (e.g., "$.details"), then append each key name to form individual key paths.
        var parentPath = basePath.substring(0, basePath.length() - "[*]".length());

        var extractExprs = nameUnion.names().stream()
                .map(name -> "json_extract({0}, '%s.%s')".formatted(parentPath, name.replace("'", "''")))
                .collect(Collectors.joining(", "));

        return table("""
                        LATERAL (SELECT unnest(list_value(%1$s)) AS "%2$s", \
                        unnest(range(%3$d)) AS "%4$s")\
                        """.formatted(extractExprs, UNNEST_FIELD, nameUnion.names().size(), ORDINAL_FIELD), parentRef)
                .as(quotedName(absoluteName));
    }

    @Override
    public SelectField<?> compileFieldTypeReference(String reference, Name typeAlias) {
        var normalized = normalizeBracketNotation(reference);
        return DSL.field("json_type({0}, {1})", field(quotedName(cteAlias, iterColumn)), inline(normalized))
                .as(typeAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias) {
        var normalized = normalizeBracketNotation(reference);
        return DSL.field("json_type({0}, {1})", field(quotedName(unnestAlias, UNNEST_FIELD)), inline(normalized))
                .as(typeAlias);
    }

    @Override
    public Field<Object> resolveJoinChildReference(String childRef) {
        var sourceRef = fieldNameToRefMap.get(childRef);
        if (sourceRef != null) {
            var normalized = normalizeBracketNotation(sourceRef);
            return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(normalized));
        }
        return field(quotedName(cteAlias, childRef));
    }

    @Override
    public Set<String> nonScalarTypeValues() {
        return NON_SCALAR_JSON_TYPES;
    }

    @Override
    public Optional<String> sourceEvaluationColumn() {
        return Optional.of(iterColumn);
    }

    @Override
    public UnnestDescriptor compileMultiValuedUnnestDescriptor(ExpressionField field, String cteAlias) {
        var fieldName = field.getFieldName();
        var reference = field.getReference();

        var parsed = JsonPathAnalyzer.analyze(reference);

        Table<?> unnestTable;
        if (parsed.filters().isEmpty()) {
            unnestTable = compileUnnestTable(reference, cteAlias, true, fieldName);
        } else {
            // Build a filtered LATERAL: unnest all elements using the basePath, then apply a WHERE
            // clause for the filter conditions and recompute ordinals as sequential 0-based indices.
            //
            // The inner unnest expands all array elements. The outer LATERAL wraps it with a WHERE
            // clause to apply the filter, and row_number() to produce sequential ordinals.
            var innerUnnest = compileUnnestTable(parsed.basePath(), cteAlias, true, fieldName + "_inner");

            var filterCondition = parsed.filters().stream()
                    .map(f -> JsonPathSourceHandler.compileFilterCondition(f, UNNEST_FIELD))
                    .reduce(Condition::and)
                    .orElseThrow();

            // Render the inner unnest as a subquery in a SELECT to capture its full SQL,
            // then wrap with WHERE and ordinal recomputation. row_number() over() has no ORDER BY
            // because DuckDB's UNNEST preserves array element order, and the LATERAL boundary
            // resets numbering per parent row.
            var innerQuery = CTX.renderInlined(CTX.selectFrom(innerUnnest));

            unnestTable = table("LATERAL (SELECT \"unnest\", (row_number() over() - 1) AS \"%s\" FROM (%s) WHERE %s)"
                            .formatted(ORDINAL_FIELD, innerQuery, CTX.renderInlined(filterCondition)))
                    .as(quotedName(fieldName));
        }

        var nestedSelects = new ArrayList<SelectField<?>>();

        // Extract value from unnested element: json_extract_string(fieldName."unnest", '$')
        nestedSelects.add(compileNestedFieldReference(fieldName, "$", DuckDbViewCompiler.fieldAlias(fieldName)));

        // Add type companion for the unnested value
        var typeAlias = quotedName(fieldName + TYPE_SUFFIX);
        nestedSelects.add(compileNestedFieldTypeReference(fieldName, "$", typeAlias));

        // Add ordinal column: fieldName."__ord" AS "fieldName.#"
        var indexColumnName = fieldName + ".#";
        nestedSelects.add(field(quotedName(fieldName, ORDINAL_FIELD)).as(quotedName(indexColumnName)));

        return new UnnestDescriptor(unnestTable, List.copyOf(nestedSelects));
    }

    /**
     * Validates that a JSONPath reference expression has valid syntax. DuckDB's
     * {@code json_extract_string} silently returns NULL for invalid JSONPath expressions, but the
     * RML spec requires that invalid reference expressions produce an error.
     *
     * <p>For {@code $}-prefixed expressions, validation is delegated to
     * {@link JsonPathAnalyzer#analyze}, which provides full ANTLR-based parsing needed for SQL
     * translation. For bare expressions, the shared
     * {@link JsonPathValidator#validateStrict(String)} is used.
     *
     * @param reference the JSONPath reference expression to validate
     * @throws IllegalArgumentException if the reference has invalid JSONPath syntax
     */
    private static void validateJsonPathSyntax(String reference) {
        if (reference.startsWith("$")) {
            // Full JSONPath expression — validate via analyzer (which also extracts
            // structural information needed by the SQL compiler)
            JsonPathAnalyzer.analyze(reference);
        } else {
            try {
                JsonPathValidator.validateStrict(reference);
            } catch (JsonPathValidationException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }
}
