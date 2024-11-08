package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.Source;

public interface SourceSupport {

    boolean supportsSource(Source source);
}
