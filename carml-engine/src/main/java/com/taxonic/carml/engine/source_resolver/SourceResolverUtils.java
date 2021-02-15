package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.model.FileSource;

import java.util.Optional;

abstract class SourceResolverUtils {

    static Optional<String> unpackFileSource(Object sourceObject) {
        if (sourceObject instanceof String) { // Standard rml:source
            return Optional.of((String) sourceObject);
        } else if (sourceObject instanceof FileSource) { // Extended Carml source
            return Optional.of(((FileSource)sourceObject).getUrl());
        } else {
            return Optional.empty();
        }
    }
}
