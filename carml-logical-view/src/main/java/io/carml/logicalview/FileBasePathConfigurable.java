package io.carml.logicalview;

import java.nio.file.Path;

/**
 * Optional interface that {@link LogicalViewEvaluatorFactory} implementations can implement to
 * receive a file base path for resolving relative file references in data sources.
 *
 * <p>When the mapper builder is configured with a file resolver ({@code fileResolver(Path)} or
 * {@code classPathResolver()}), factories implementing this interface will have their base path set
 * automatically before view matching occurs.
 */
public interface FileBasePathConfigurable {

    /**
     * Sets the base path used for resolving relative file references in data sources.
     *
     * @param basePath the absolute base path for file resolution
     */
    void setFileBasePath(Path basePath);
}
