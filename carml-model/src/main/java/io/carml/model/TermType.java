package io.carml.model;

/**
 * The term type of an RML term map, determining how generated RDF terms are encoded.
 */
public enum TermType {

    /**
     * IRI term type (RFC 3987). Percent-encodes unsafe ASCII characters in template reference values,
     * but preserves Unicode code points in the {@code ucschar} range.
     */
    IRI,

    /**
     * URI term type (RFC 3986). Percent-encodes both unsafe ASCII characters and non-ASCII (Unicode)
     * characters as UTF-8 byte sequences in template reference values.
     */
    URI,

    /**
     * Unsafe IRI term type. Produces IRIs without any percent-encoding of template reference values.
     * The resulting IRIs may contain characters that are invalid per RFC 3987.
     */
    UNSAFE_IRI,

    BLANK_NODE,

    LITERAL
}
