package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.DcatDistribution;
import io.carml.model.Source;
import io.carml.model.impl.CarmlRelativePathSource;
import io.netty.handler.codec.http.HttpMethod;
import java.io.SequenceInputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.AllArgsConstructor;
import reactor.netty.http.client.HttpClient;

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
            var url = new URL(downloadUrl);
            if (url.getProtocol().equals("file")) {
                return FileResolver.of()
                        .apply(CarmlRelativePathSource.of(Paths.get(url.toURI()).toString()));
            } else if (url.getProtocol().equals("http") || url.getProtocol().equals("https")) {
                return Optional.of(HttpClient.create()
                        .followRedirect(true)
                        .request(HttpMethod.GET)
                        .uri(url.toURI())
                        .responseContent()
                        .asInputStream()
                        .reduce(SequenceInputStream::new)
                        .map(inputStream ->
                                Decompressor.getInstance().apply(inputStream, dcatDistribution.getCompression()))
                        .doOnError(throwable -> {
                            throw new SourceResolverException(
                                    String.format("Failed to download source%n%s", exception(dcatDistribution)),
                                    throwable);
                        }));
            } else {
                throw new SourceResolverException(String.format(
                        "Unsupported protocol %s for source%n%s", url.getProtocol(), exception(dcatDistribution)));
            }
        } catch (MalformedURLException | URISyntaxException urlException) {
            throw new SourceResolverException(
                    String.format(
                            "Encountered malformed URL %s for source%n%s", downloadUrl, exception(dcatDistribution)),
                    urlException);
        }
    }
}
