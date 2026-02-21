package io.carml.logicalview;

import java.util.List;
import java.util.Optional;

/**
 * Describes a single discoverable field in a data source. Fields may be nested (for JSON objects,
 * XML elements with children) and may be iterable (for JSON arrays, XML repeating elements).
 *
 * <p>This is a recursive structure: a field descriptor can contain nested field descriptors,
 * enabling representation of arbitrarily deep source schemas.
 *
 * @param name the field name as it appears in the source
 * @param type the type name of the field (e.g., "string", "integer", "object"), or null if unknown
 * @param nullable whether the field can contain null values, or null if unknown
 * @param iterable whether the field represents a repeating/array structure
 * @param nestedFields nested field descriptors for structured fields; empty list if the field is a
 *     leaf
 */
public record FieldDescriptor(
        String name, String type, Boolean nullable, boolean iterable, List<FieldDescriptor> nestedFields) {

    /**
     * Compact constructor ensuring immutability of the nested fields list.
     */
    public FieldDescriptor {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Field name must not be null or empty");
        }
        nestedFields = nestedFields != null ? List.copyOf(nestedFields) : List.of();
    }

    /**
     * Returns the optional type name of this field.
     *
     * @return an optional containing the type name, or empty if unknown
     */
    public Optional<String> getType() {
        return Optional.ofNullable(type);
    }

    /**
     * Returns whether this field is nullable.
     *
     * @return an optional containing the nullability, or empty if unknown
     */
    public Optional<Boolean> getNullable() {
        return Optional.ofNullable(nullable);
    }

    /**
     * Creates a leaf field descriptor with no nested fields.
     *
     * @param name the field name
     * @param type the type name, or null if unknown
     * @param nullable whether the field is nullable, or null if unknown
     * @return a leaf field descriptor
     */
    public static FieldDescriptor leaf(String name, String type, Boolean nullable) {
        return new FieldDescriptor(name, type, nullable, false, List.of());
    }

    /**
     * Creates a leaf field descriptor with a known type.
     *
     * @param name the field name
     * @param type the type name
     * @return a leaf field descriptor
     */
    public static FieldDescriptor leaf(String name, String type) {
        return new FieldDescriptor(name, type, null, false, List.of());
    }

    /**
     * Creates a leaf field descriptor with no type information.
     *
     * @param name the field name
     * @return a leaf field descriptor
     */
    public static FieldDescriptor leaf(String name) {
        return new FieldDescriptor(name, null, null, false, List.of());
    }

    /**
     * Creates a structured (non-iterable) field descriptor with nested fields.
     *
     * @param name the field name
     * @param nestedFields the nested field descriptors
     * @return a structured field descriptor
     */
    public static FieldDescriptor structured(String name, List<FieldDescriptor> nestedFields) {
        return new FieldDescriptor(name, "object", null, false, nestedFields);
    }

    /**
     * Creates an iterable field descriptor with nested fields describing the element structure.
     *
     * @param name the field name
     * @param nestedFields the nested field descriptors for each element
     * @return an iterable field descriptor
     */
    public static FieldDescriptor iterable(String name, List<FieldDescriptor> nestedFields) {
        return new FieldDescriptor(name, "array", null, true, nestedFields);
    }

    /**
     * Creates an iterable leaf field descriptor (e.g., an array of scalars).
     *
     * @param name the field name
     * @param elementType the type of each element
     * @return an iterable leaf field descriptor
     */
    public static FieldDescriptor iterableLeaf(String name, String elementType) {
        return new FieldDescriptor(name, elementType, null, true, List.of());
    }

    /**
     * Returns whether this field has nested fields.
     *
     * @return true if this field contains nested field descriptors
     */
    public boolean hasNestedFields() {
        return !nestedFields.isEmpty();
    }
}
