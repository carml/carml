package io.carml.logicalview;

import io.carml.model.Mapping;

/**
 * Optional interface that {@link LogicalViewEvaluatorFactory} implementations can implement to
 * receive the active {@link Mapping} so anchor-resolution semantics (e.g. {@code rml:root
 * rml:MappingDirectory}) can be honored. Engines that do not consult the mapping context for
 * source resolution may omit this interface.
 *
 * <p>The mapper builder calls {@link #setMapping(Mapping)} on every matching factory before view
 * matching begins, so the supplied mapping is the same reference used by the file resolver chain.
 */
public interface MappingConfigurable {

    /**
     * Sets the active mapping used for anchor-based source resolution.
     *
     * @param mapping the mapping that holds the file paths consulted when a source declares
     *     {@code rml:root rml:MappingDirectory}; may be {@code null} when the caller has no
     *     mapping context (e.g. ad-hoc programmatic invocations)
     */
    void setMapping(Mapping mapping);
}
