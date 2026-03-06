package io.carml.logicalview;

import java.util.List;

/**
 * Describes the structure of a data source as discovered by a {@link SourceIntrospector}. A source
 * schema consists of a list of top-level {@link FieldDescriptor}s representing the available fields
 * in the source, and a list of {@link ConstraintDescriptor}s representing discovered constraints
 * (primary keys, unique, foreign keys, not-null).
 *
 * @param fields the top-level field descriptors of the source
 * @param constraints the constraint descriptors discovered from the source
 */
public record SourceSchema(List<FieldDescriptor> fields, List<ConstraintDescriptor> constraints) {

    /**
     * Compact constructor ensuring immutability of the fields and constraints lists.
     */
    public SourceSchema {
        fields = fields != null ? List.copyOf(fields) : List.of();
        constraints = constraints != null ? List.copyOf(constraints) : List.of();
    }

    /**
     * Creates a source schema with fields only and no constraints.
     *
     * @param fields the field descriptors
     */
    public SourceSchema(List<FieldDescriptor> fields) {
        this(fields, List.of());
    }

    /**
     * Creates an empty source schema with no fields and no constraints.
     *
     * @return an empty source schema
     */
    public static SourceSchema empty() {
        return new SourceSchema(List.of(), List.of());
    }

    /**
     * Returns whether this schema has any fields.
     *
     * @return true if this schema contains at least one field descriptor
     */
    public boolean hasFields() {
        return !fields.isEmpty();
    }

    /**
     * Returns whether this schema has any constraints.
     *
     * @return true if this schema contains at least one constraint descriptor
     */
    public boolean hasConstraints() {
        return !constraints.isEmpty();
    }
}
