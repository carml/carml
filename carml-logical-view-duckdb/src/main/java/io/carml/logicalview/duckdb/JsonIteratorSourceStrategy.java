package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import io.carml.jsonpath.JsonPathNormalizer;
import io.carml.jsonpath.JsonPathValidationException;
import io.carml.jsonpath.JsonPathValidator;
import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
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

    @SuppressWarnings("UnstableApiUsage")
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
            // Invalid JSONPath syntax cannot be multivalued. The error will surface
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
    public Field<?> resolveJoinChildExpression(ExpressionMap childMap) {
        return DuckDbViewCompiler.compileJoinExpressionMap(
                childMap, this::resolveChildReferenceField, this::compileTemplateReference);
    }

    private Field<?> resolveChildReferenceField(String childRef) {
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

        // Multivalued ExpressionFields contribute join-projected values. When the reference
        // produces zero elements for a given parent row, the parent must still survive (with
        // the field key projected as NULL) so triples that depend only on other fields are
        // preserved. This mirrors the standalone-view evaluation in
        // DefaultLogicalViewEvaluator (see the values.isEmpty() branch in evaluateFields and
        // the equivalent guard in matchAndExtendRegular). The empty-preserving LATERAL wraps
        // the inner unnest body with a NOT EXISTS fallback that emits a (NULL, 0) sentinel
        // row when the inner result is empty.
        var unnestTable = compileEmptyPreservingMultiValuedUnnest(parsed, reference, cteAlias, fieldName);

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
     * Builds a {@code LATERAL} subquery for a multivalued {@link ExpressionField} that preserves
     * the parent row when the unnest result is empty. When the inner unnest emits zero rows
     * (empty array, all elements filtered out, slice/index union with no matches), a single
     * sentinel row with {@code unnest = NULL} and {@code __ord = 0} is emitted instead, so the
     * surrounding comma-cross-join in {@code DuckDbViewCompiler.buildFromClause} does not drop
     * the parent.
     *
     * <p>This semantic is specific to multivalued ExpressionFields used in join projections
     * and must NOT be applied to {@code IterableField} unnest tables, where empty iterables
     * are intended to drop the parent row (mirroring {@code evaluateIterableField} in
     * {@code DefaultLogicalViewEvaluator}). The {@link #compileUnnestTable} strategy method
     * therefore retains its original drop-on-empty behavior and is used unchanged for
     * {@code IterableField}.
     */
    private Table<?> compileEmptyPreservingMultiValuedUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String reference, String cteAlias, String fieldName) {
        // Skip the empty-preservation wrapping when the inner body provably emits at least one
        // row regardless of input — name unions (e.g., $.['a','b']) always emit N >= 1 rows,
        // and the single-value path (non-array selectors) always emits exactly one row via
        // list_value(...). Wrapping these would introduce an unnecessary UNION ALL whose
        // optimizer-driven row reordering disturbs source order for downstream tests that
        // rely on natural iteration order.
        if (alwaysEmitsRow(parsed)) {
            return compileUnnestTable(reference, cteAlias, true, fieldName);
        }

        var innerBody = buildMultiValuedInnerBody(parsed, reference, cteAlias, fieldName);
        var sql = """
                LATERAL (\
                %1$s \
                UNION ALL \
                SELECT NULL AS "%2$s", 0 AS "%3$s" \
                WHERE NOT EXISTS (SELECT 1 FROM (%1$s) "_empty_check")\
                )""".formatted(innerBody, UNNEST_FIELD, ORDINAL_FIELD);
        return table(sql).as(quotedName(fieldName));
    }

    /**
     * Returns {@code true} when the inner unnest body for the given parsed JSONPath provably
     * emits at least one row regardless of the parent JSON value:
     * <ul>
     *   <li>Single-value paths (no array result, no filters) — wrapped in {@code list_value(...)}
     *       and always emit exactly one row.</li>
     *   <li>Name unions (e.g., {@code $.['a','b']}) — always emit one row per name in the union
     *       (range count is fixed at compile time).</li>
     * </ul>
     * Other paths (standard array, slice, index union, filter) may emit zero rows and therefore
     * need the empty-preservation wrapping.
     */
    private static boolean alwaysEmitsRow(JsonPathAnalyzer.ParsedJsonPath parsed) {
        if (!parsed.filters().isEmpty()) {
            return false;
        }
        if (!isArrayResult(parsed)) {
            return true;
        }
        if (parsed.unions().isEmpty()) {
            return false;
        }
        var union = parsed.unions().get(parsed.unions().size() - 1);
        return union instanceof JsonPathAnalyzer.NameUnion;
    }

    /**
     * Builds the inner SELECT body (without the surrounding {@code LATERAL (...)}) for a
     * multivalued reference. The returned SQL fragment correlates to the parent row via the
     * outer LATERAL boundary, so it must be embedded inside a {@code LATERAL (...)} subquery
     * by the caller. The body emits two columns named {@code "unnest"} and {@code "__ord"}.
     */
    private String buildMultiValuedInnerBody(
            JsonPathAnalyzer.ParsedJsonPath parsed, String reference, String cteAlias, String fieldName) {
        if (parsed.filters().isEmpty()) {
            // No filter expression — body shape depends on selector (slice / index union /
            // name union / standard array / single value). Render the existing LATERAL table
            // and strip the outer "LATERAL (...)" wrapping so we can re-wrap with the
            // empty-preserving variant. The inner LATERAL body is fully self-contained
            // (correlations are resolved against the outer LATERAL boundary established by
            // the caller).
            var lateralTable = compileUnnestTable(reference, cteAlias, true, fieldName);
            return stripLateralWrapper(CTX.renderInlined(CTX.selectFrom(lateralTable)));
        }

        // Filter expression path: unnest the basePath, then apply a WHERE on the filter
        // condition, recomputing ordinals via row_number() over the surviving rows.
        var innerUnnest = compileUnnestTable(parsed.basePath(), cteAlias, true, fieldName + "_inner");
        var filterCondition = parsed.filters().stream()
                .map(f -> JsonPathSourceHandler.compileFilterCondition(f, UNNEST_FIELD))
                .reduce(Condition::and)
                .orElseThrow();
        var innerQuery = CTX.renderInlined(CTX.selectFrom(innerUnnest));
        return "SELECT \"%s\", (row_number() over() - 1) AS \"%s\" FROM (%s) WHERE %s"
                .formatted(UNNEST_FIELD, ORDINAL_FIELD, innerQuery, CTX.renderInlined(filterCondition));
    }

    /**
     * Strips the outer {@code SELECT * FROM LATERAL (<body>) "alias"} wrapper produced by
     * rendering a LATERAL table via {@code CTX.selectFrom(...)}. Returns just the inner body
     * so it can be embedded inside a different LATERAL.
     *
     * <p>The rendered form for a LATERAL table is approximately:
     * {@code select * from LATERAL (<body>) "alias"}. The body is everything between the
     * first {@code LATERAL (} and the matching closing {@code )}. The leading
     * {@code select * from } and the trailing {@code "alias"} (or unquoted alias) are
     * discarded.
     */
    private static String stripLateralWrapper(String renderedSelect) {
        var lateralMarker = "LATERAL (";
        var start = renderedSelect.indexOf(lateralMarker);
        if (start < 0) {
            throw new IllegalStateException("Expected rendered LATERAL table, but got: %s".formatted(renderedSelect));
        }
        var bodyStart = start + lateralMarker.length();
        var depth = 1;
        for (var i = bodyStart; i < renderedSelect.length(); i++) {
            var c = renderedSelect.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return renderedSelect.substring(bodyStart, i);
                }
            }
        }
        throw new IllegalStateException("Unbalanced parentheses in rendered LATERAL: %s".formatted(renderedSelect));
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
