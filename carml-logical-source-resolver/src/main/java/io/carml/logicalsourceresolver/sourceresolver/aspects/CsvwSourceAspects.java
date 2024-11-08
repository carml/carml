package io.carml.logicalsourceresolver.sourceresolver.aspects;

import static io.carml.logicalsourceresolver.sourceresolver.GetHttpUrl.toUrl;

import com.google.auto.service.AutoService;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.net.URL;
import java.util.Optional;
import java.util.function.Function;
import lombok.NoArgsConstructor;

@AutoService(FileSourceAspects.class)
@NoArgsConstructor
public class CsvwSourceAspects extends AbstractFileSourceAspects {

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof CsvwTable;
    }

    @Override
    public int getPriority() {
        return 30;
    }

    @Override
    public Optional<Function<Source, Optional<URL>>> getUrl() {
        return Optional.of(source -> {
            if (source instanceof CsvwTable csvwTable && csvwTable.getUrl() != null) {
                return Optional.of(toUrl(csvwTable.getUrl(), csvwTable));
            }

            return Optional.empty();
        });
    }
}
