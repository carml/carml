package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.util.Optional;

public class CsvwResolver implements SourceResolver {

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof CsvwTable;
    }

    @Override
    public Optional<Object> apply(Source source) {
        return Optional.of(source);
    }
}
