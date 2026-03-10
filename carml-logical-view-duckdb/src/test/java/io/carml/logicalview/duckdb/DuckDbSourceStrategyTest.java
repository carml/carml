package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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

        private final ColumnSourceStrategy strategy = new ColumnSourceStrategy(CTE_ALIAS);

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
