package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.logicalsourceresolver.sourceresolver.GetHttpUrl.toUrl;
import static io.carml.util.LogUtil.exception;

import io.carml.model.DcatDistribution;
import io.carml.model.Source;
import io.carml.model.impl.CarmlRelativePathSource;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(staticName = "of")
public class DcatDistributionResolver implements SourceResolver {

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof DcatDistribution;
    }

    @Override
    public Optional<Object> apply(Source source) {
        if (source instanceof DcatDistribution dcatDistribution) {
            if (dcatDistribution.getDownloadUrl() != null) {
                return handleDownloadUrl(dcatDistribution.getDownloadUrl().stringValue(), dcatDistribution);
            }
            if (dcatDistribution.getAccessUrl() != null) {
                return handleDownloadUrl(dcatDistribution.getAccessUrl().stringValue(), dcatDistribution);
            }
        }

        return Optional.empty();
    }

    private Optional<Object> handleDownloadUrl(String downloadUrl, DcatDistribution dcatDistribution) {
        try {
            var url = toUrl(downloadUrl, dcatDistribution);
            if (url.getProtocol().equals("file")) {
                return FileResolver.of()
                        .apply(CarmlRelativePathSource.of(Paths.get(url.toURI()).toString()));
            } else if (url.getProtocol().equals("http") || url.getProtocol().equals("https")) {
                return Optional.of(GetHttpUrl.getInstance().apply(url).map(inputStream -> Decompressor.getInstance()
                        .apply(inputStream, dcatDistribution.getCompression())));
            } else {
                throw new SourceResolverException(String.format(
                        "Unsupported protocol %s for source%n%s", url.getProtocol(), exception(dcatDistribution)));
            }
        } catch (URISyntaxException uriSyntaxException) {
            throw new SourceResolverException(
                    String.format(
                            "Encountered malformed URI %s for source%n%s", downloadUrl, exception(dcatDistribution)),
                    uriSyntaxException);
        }
    }
}
