package io.carml.logicalview;

import java.util.List;

/**
 * Describes a constraint discovered during source introspection. Constraints include primary keys,
 * unique constraints, foreign keys, and not-null constraints. These correspond to
 * {@link io.carml.model.StructuralAnnotation} types and can be used to auto-supplement structural
 * annotations not explicitly declared in the RML-LV mapping.
 *
 * @param type the type of constraint
 * @param columns the column names involved in this constraint
 * @param referencedTable the referenced table name for foreign key constraints; {@code null} for
 *     other constraint types
 * @param referencedColumns the referenced column names for foreign key constraints; empty list for
 *     other constraint types
 */
public record ConstraintDescriptor(
        ConstraintType type, List<String> columns, String referencedTable, List<String> referencedColumns) {

    /**
     * The type of constraint.
     */
    public enum ConstraintType {
        PRIMARY_KEY,
        UNIQUE,
        FOREIGN_KEY,
        NOT_NULL
    }

    /**
     * Compact constructor ensuring immutability of the column lists.
     */
    public ConstraintDescriptor {
        if (type == null) {
            throw new IllegalArgumentException("Constraint type must not be null");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Constraint columns must not be null or empty");
        }
        columns = List.copyOf(columns);
        referencedColumns = referencedColumns != null ? List.copyOf(referencedColumns) : List.of();
    }

    /**
     * Creates a primary key constraint descriptor.
     *
     * @param columns the column names forming the primary key
     * @return a primary key constraint descriptor
     */
    public static ConstraintDescriptor primaryKey(List<String> columns) {
        return new ConstraintDescriptor(ConstraintType.PRIMARY_KEY, columns, null, List.of());
    }

    /**
     * Creates a unique constraint descriptor.
     *
     * @param columns the column names forming the unique constraint
     * @return a unique constraint descriptor
     */
    public static ConstraintDescriptor unique(List<String> columns) {
        return new ConstraintDescriptor(ConstraintType.UNIQUE, columns, null, List.of());
    }

    /**
     * Creates a foreign key constraint descriptor.
     *
     * @param columns the column names forming the foreign key
     * @param referencedTable the referenced table name
     * @param referencedColumns the referenced column names
     * @return a foreign key constraint descriptor
     */
    public static ConstraintDescriptor foreignKey(
            List<String> columns, String referencedTable, List<String> referencedColumns) {
        return new ConstraintDescriptor(ConstraintType.FOREIGN_KEY, columns, referencedTable, referencedColumns);
    }

    /**
     * Creates a not-null constraint descriptor.
     *
     * @param column the column name with the not-null constraint
     * @return a not-null constraint descriptor
     */
    public static ConstraintDescriptor notNull(String column) {
        return new ConstraintDescriptor(ConstraintType.NOT_NULL, List.of(column), null, List.of());
    }
}
