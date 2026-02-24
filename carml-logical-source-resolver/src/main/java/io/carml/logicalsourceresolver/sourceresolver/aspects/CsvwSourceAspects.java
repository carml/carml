package io.carml.logicalsourceresolver.sourceresolver.aspects;

import static io.carml.logicalsourceresolver.sourceresolver.GetHttpUrl.toUrl;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.sourceresolver.PathRelativeTo;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.net.MalformedURLException;
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
                if (isAbsoluteUrl(csvwTable.getUrl())) {
                    return Optional.of(toUrl(csvwTable.getUrl(), csvwTable));
                }
            }

            return Optional.empty();
        });
    }

    @Override
    public Optional<Function<Source, Optional<String>>> getPathString() {
        return Optional.of(source -> {
            if (source instanceof CsvwTable csvwTable && csvwTable.getUrl() != null) {
                if (!isAbsoluteUrl(csvwTable.getUrl())) {
                    return Optional.of(csvwTable.getUrl());
                }
            }

            return Optional.empty();
        });
    }

    @Override
    public Optional<Function<Source, Optional<PathRelativeTo>>> getPathRelativeTo() {
        return Optional.of(source -> {
            if (source instanceof CsvwTable) {
                return Optional.of(PathRelativeTo.MAPPING_DIRECTORY);
            }

            return Optional.empty();
        });
    }

    private static boolean isAbsoluteUrl(String urlString) {
        try {
            new URL(urlString);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }
}
