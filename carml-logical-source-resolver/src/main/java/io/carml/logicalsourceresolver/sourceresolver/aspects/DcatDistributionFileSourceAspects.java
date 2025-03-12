package io.carml.logicalsourceresolver.sourceresolver.aspects;

import static io.carml.logicalsourceresolver.sourceresolver.GetHttpUrl.toUrl;

import com.google.auto.service.AutoService;
import io.carml.model.DcatDistribution;
import io.carml.model.Source;
import java.net.URL;
import java.util.Optional;
import java.util.function.Function;
import lombok.NoArgsConstructor;

@AutoService(FileSourceAspects.class)
@NoArgsConstructor
public class DcatDistributionFileSourceAspects extends AbstractFileSourceAspects {

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof DcatDistribution;
    }

    @Override
    public int getPriority() {
        return 20;
    }

    @Override
    public Optional<Function<Source, Optional<URL>>> getUrl() {
        return Optional.of(source -> {
            if (source instanceof DcatDistribution dcatDistribution) {
                if (dcatDistribution.getDownloadUrl() != null) {
                    return Optional.of(toUrl(dcatDistribution.getDownloadUrl().stringValue(), dcatDistribution));
                }
                if (dcatDistribution.getAccessUrl() != null) {
                    return Optional.of(toUrl(dcatDistribution.getAccessUrl().stringValue(), dcatDistribution));
                }
            }
            return Optional.empty();
        });
    }
}
