package io.carml.logicalview.duckdb;

import java.util.List;
import org.jooq.SelectField;
import org.jooq.Table;

/**
 * Describes an UNNEST cross-join derived from an {@link io.carml.model.IterableField} or a
 * multi-valued {@link io.carml.model.ExpressionField}.
 *
 * @param unnestTable the jOOQ table expression for the UNNEST
 * @param nestedSelects the SELECT fields for the nested expression fields
 */
record UnnestDescriptor(Table<?> unnestTable, List<SelectField<?>> nestedSelects) {}
