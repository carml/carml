package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(staticName = "of")
public class CsvwSourceResolver implements SourceResolver {

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof CsvwTable;
    }

    @Override
    public Optional<Object> apply(Source source) {
        return Optional.of(source);
    }
}
