package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.Source;
import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver<T> extends Function<Source, Optional<ResolvedSource<T>>>, SourceSupport {}
