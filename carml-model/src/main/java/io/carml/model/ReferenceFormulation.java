package io.carml.model;

import java.util.Optional;

public interface ReferenceFormulation extends Resource {

    /**
     * The spec-mandated default iterator for this reference formulation, if any. Per
     * {@code rml-io/spec/section/source-vocabulary.md}, a logical source whose iterator is omitted
     * should fall back to this default. Returns empty for formulations where iteration is implicit
     * (e.g. CSV row-based, SQL table) or where a default is undefined.
     *
     * <p>The return type is {@link Object} rather than {@code String} to leave room for custom or
     * future formulations whose iterator representations are not plain strings (e.g. structured
     * configuration objects).
     */
    default Optional<Object> getDefaultIterator() {
        return Optional.empty();
    }
}
