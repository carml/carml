package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.model.ExpressionField;
import io.carml.model.Field;
import java.util.Set;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DuckDbSourceStrategyTest {

    private static final String CTE_ALIAS = "view_source";

    private static final String ITER_COLUMN = "__iter";

    private static final org.jooq.DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    @Nested
    class ColumnStrategyTests {

        private final ColumnSourceStrategy strategy =
                new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.NONE);

        @Test
        void isMultiValuedReference_alwaysReturnsFalse() {
            assertThat(strategy.isMultiValuedReference("items"), is(false));
            assertThat(strategy.isMultiValuedReference("$.items[*]"), is(false));
            assertThat(strategy.isMultiValuedReference(null), is(false));
        }

        @Test
        void compileFieldReference_producesQualifiedColumnRef() {
            var result = strategy.compileFieldReference("name", DSL.name("name"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"view_source\".\"name\" \"name\""));
        }

        @Test
        void compileTemplateReference_producesQualifiedColumnRef() {
            var result = strategy.compileTemplateReference("id");
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"view_source\".\"id\""));
        }

        @Test
        void compileNestedFieldReference_producesUnnestAliasQualifiedRef() {
            var result = strategy.compileNestedFieldReference("item", "type", DSL.quotedName("item.item_type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
        }

        @Test
        void compileUnnestTable_rootLevel_producesLateralUnnestWithOrdinal() {
            var result = strategy.compileUnnestTable("items", CTE_ALIAS, true, "item");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("range(len(\"view_source\".\"items\"))"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileUnnestTable_nestedLevel_producesLateralUnnestWithOrdinal() {
            var result = strategy.compileUnnestTable("details", "item", false, "detail");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"item\".\"details\")"));
            assertThat(sql, containsString("range(len(\"item\".\"details\"))"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void resolveJoinChildReference_producesQualifiedColumnRef() {
            var result = strategy.resolveJoinChildReference("dept_id");
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"view_source\".\"dept_id\""));
        }

        @Test
        void compileFieldTypeReference_withoutTypeCompanions_producesCastNull() {
            var result = strategy.compileFieldTypeReference("name", DSL.quotedName("name.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("cast(null as varchar) \"name.__type\""));
        }

        @Test
        void compileFieldTypeReference_withInnerViewMode_producesColumnProjection() {
            var viewOnViewStrategy =
                    new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.INNER_VIEW);
            var result = viewOnViewStrategy.compileFieldTypeReference("name", DSL.quotedName("name.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"view_source\".\"name.__type\" \"name.__type\""));
        }

        @Test
        void compileNestedFieldTypeReference_withoutTypeCompanions_producesCastNull() {
            var result = strategy.compileNestedFieldTypeReference("item", "type", DSL.quotedName("item.type.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("cast(null as varchar) \"item.type.__type\""));
        }

        @Test
        void compileNestedFieldTypeReference_withInnerViewMode_producesColumnProjection() {
            var viewOnViewStrategy =
                    new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.INNER_VIEW);
            var result = viewOnViewStrategy.compileNestedFieldTypeReference(
                    "item", "type", DSL.quotedName("item.type.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"item\".\"type.__type\" \"item.type.__type\""));
        }

        @Test
        void compileFieldTypeReference_sqlTypeof_producesTypeofExpression() {
            var sqlStrategy = new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.SQL_TYPEOF);
            var result = sqlStrategy.compileFieldTypeReference("age", DSL.quotedName("age.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("typeof(\"view_source\".\"age\") \"age.__type\""));
        }

        @Test
        void compileNestedFieldTypeReference_sqlTypeof_producesTypeofExpression() {
            var sqlStrategy = new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.SQL_TYPEOF);
            var result =
                    sqlStrategy.compileNestedFieldTypeReference("item", "price", DSL.quotedName("item.price.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("typeof(\"item\".\"price\") \"item.price.__type\""));
        }
    }

    @Nested
    class JsonIteratorStrategyTests {

        private JsonIteratorSourceStrategy createStrategy(Set<ExpressionField> fields) {
            @SuppressWarnings("unchecked")
            var fieldSet = (Set<Field>) (Set<?>) fields;
            return JsonIteratorSourceStrategy.create(fieldSet, CTE_ALIAS, ITER_COLUMN);
        }

        private ExpressionField expressionField(String fieldName, String reference) {
            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn(fieldName);
            lenient().when(field.getReference()).thenReturn(reference);
            return field;
        }

        @Test
        void isMultiValuedReference_withArrayWildcard_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.items[*]"), is(true));
            assertThat(strategy.isMultiValuedReference("$.people[*].name"), is(true));
        }

        @Test
        void isMultiValuedReference_withFilterExpression_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.people[?(@.age>18)]"), is(true));
        }

        @Test
        void isMultiValuedReference_withDeepScan_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$..name"), is(true));
        }

        @Test
        void isMultiValuedReference_withChildWildcard_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.*"), is(true));
        }

        @Test
        void isMultiValuedReference_withSlice_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.items[0:3]"), is(true));
        }

        @Test
        void isMultiValuedReference_withIndexUnion_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.items[0,2]"), is(true));
        }

        @Test
        void isMultiValuedReference_withNameUnion_returnsTrue() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$['name','age']"), is(true));
        }

        @Test
        void isMultiValuedReference_withoutArrayWildcard_returnsFalse() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference("$.name"), is(false));
            assertThat(strategy.isMultiValuedReference("name"), is(false));
        }

        @Test
        void isMultiValuedReference_withNull_returnsFalse() {
            var strategy = createStrategy(Set.of());
            assertThat(strategy.isMultiValuedReference(null), is(false));
        }

        @Test
        void compileFieldReference_producesJsonExtractString() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileFieldReference("name", DSL.name("name"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", 'name') \"name\""));
        }

        @Test
        void compileTemplateReference_producesJsonExtractString() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileTemplateReference("id");
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", 'id')"));
        }

        @Test
        void compileNestedFieldReference_producesJsonExtractStringFromUnnest() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileNestedFieldReference("item", "type", DSL.quotedName("item.item_type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_extract_string(\"item\".\"unnest\", 'type') \"item.item_type\""));
        }

        @Test
        void compileUnnestTable_rootLevelWithArrayIterator_producesLateralJsonExtractWithOrdinal() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileUnnestTable("$.items[*]", CTE_ALIAS, true, "item");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(json_extract(\"view_source\".\"__iter\", '$.items[*]'))"));
            assertThat(sql, containsString("range(len(json_extract(\"view_source\".\"__iter\", '$.items[*]')))"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileUnnestTable_rootLevelWithoutArrayIterator_producesLateralListValueWithOrdinal() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileUnnestTable("$.address", CTE_ALIAS, true, "addr");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(
                    sql, containsString("unnest(list_value(json_extract(\"view_source\".\"__iter\", '$.address')))"));
            assertThat(sql, containsString("list_value(0)"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileUnnestTable_nestedLevelWithArrayIterator_producesLateralJsonExtractFromUnnestWithOrdinal() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileUnnestTable("$.details[*]", "item", false, "detail");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(json_extract(\"item\".\"unnest\", '$.details[*]'))"));
            assertThat(sql, containsString("range(len(json_extract(\"item\".\"unnest\", '$.details[*]')))"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileUnnestTable_nestedLevelWithoutArrayIterator_producesLateralListValueFromUnnestWithOrdinal() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileUnnestTable("$.address", "item", false, "addr");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(list_value(json_extract(\"item\".\"unnest\", '$.address')))"));
            assertThat(sql, containsString("list_value(0)"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileUnnestTable_withDeepScan_throwsUnsupported() {
            var strategy = createStrategy(Set.of());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> strategy.compileUnnestTable("$..name", CTE_ALIAS, true, "names"));
        }

        @Test
        void compileUnnestTable_withChildWildcard_throwsUnsupported() {
            var strategy = createStrategy(Set.of());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> strategy.compileUnnestTable("$.*", CTE_ALIAS, true, "children"));
        }

        @Test
        void compileUnnestTable_withFilterExpression_passesRawJsonPathToDuckDb() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileUnnestTable("$.items[?(@.active==true)]", CTE_ALIAS, true, "items");
            var sql = CTX.selectFrom(result).getSQL();
            assertThat(sql, containsString("LATERAL"));
            // Raw JSONPath with filter is passed through so DuckDB applies the filter natively
            assertThat(sql, containsString("json_extract(\"view_source\".\"__iter\", '$.items[?(@.active==true)]')"));
            assertThat(sql, containsString("\"__ord\""));
        }

        @Test
        void compileFieldTypeReference_producesJsonType() {
            var strategy = createStrategy(Set.of());
            var result = strategy.compileFieldTypeReference("name", DSL.quotedName("name.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_type(\"view_source\".\"__iter\", 'name') \"name.__type\""));
        }

        @Test
        void compileNestedFieldTypeReference_producesJsonTypeFromUnnest() {
            var strategy = createStrategy(Set.of());
            var result =
                    strategy.compileNestedFieldTypeReference("item", "type", DSL.quotedName("item.item_type.__type"));
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_type(\"item\".\"unnest\", 'type') \"item.item_type.__type\""));
        }

        @Test
        void resolveJoinChildReference_withMapping_producesJsonExtractString() {
            var strategy = createStrategy(Set.of(expressionField("dept_id", "department_id")));
            var result = strategy.resolveJoinChildReference("dept_id");
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", 'department_id')"));
        }

        @Test
        void resolveJoinChildReference_withoutMapping_producesDirectColumnRef() {
            var strategy = createStrategy(Set.of(expressionField("other_field", "other_ref")));
            var result = strategy.resolveJoinChildReference("dept_id");
            var sql = CTX.select(result).getSQL();
            assertThat(sql, containsString("\"view_source\".\"dept_id\""));
        }
    }
}
