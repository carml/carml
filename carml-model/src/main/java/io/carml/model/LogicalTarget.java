package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface LogicalTarget extends Resource {

    Target getTarget();

    /**
     * Returns the {@code rml:serialization} IRI declared directly on this LogicalTarget, or
     * {@code null} if none. Per the RML-IO specification, serialization is primarily a property of
     * {@code rml:LogicalTarget}; callers resolving an effective serialization should fall back to
     * the nested {@link Target#getSerialization()} when this returns {@code null}.
     */
    IRI getSerialization();

    /**
     * Returns the {@code rml:encoding} IRI declared directly on this LogicalTarget, or {@code null}
     * if none. Same LogicalTarget-over-Target precedence as {@link #getSerialization()}.
     */
    IRI getEncoding();

    /**
     * Returns the {@code rml:compression} IRI declared directly on this LogicalTarget, or
     * {@code null} if none. Same LogicalTarget-over-Target precedence as
     * {@link #getSerialization()}.
     */
    IRI getCompression();
}
