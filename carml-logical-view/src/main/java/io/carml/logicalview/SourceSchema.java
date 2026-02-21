package io.carml.logicalview;

import java.util.List;

/**
 * Describes the structure of a data source as discovered by a {@link SourceIntrospector}. A source
 * schema consists of a list of top-level {@link FieldDescriptor}s representing the available fields
 * in the source.
 *
 * @param fields the top-level field descriptors of the source
 */
public record SourceSchema(List<FieldDescriptor> fields) {

    /**
     * Compact constructor ensuring immutability of the fields list.
     */
    public SourceSchema {
        fields = fields != null ? List.copyOf(fields) : List.of();
    }

    /**
     * Creates an empty source schema with no fields.
     *
     * @return an empty source schema
     */
    public static SourceSchema empty() {
        return new SourceSchema(List.of());
    }

    /**
     * Returns whether this schema has any fields.
     *
     * @return true if this schema contains at least one field descriptor
     */
    public boolean hasFields() {
        return !fields.isEmpty();
    }
}
