package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.carml.model.DcatDistribution;
import io.carml.model.Source;
import io.carml.model.impl.CarmlRelativePathSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;

@AllArgsConstructor(staticName = "of")
public class DcatDistributionResolver implements SourceResolver {

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

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
                return FileResolver.getInstance()
                        .apply(CarmlRelativePathSource.of(Paths.get(url.toURI()).toString()));
            } else if (url.getProtocol().equals("http") || url.getProtocol().equals("https")) {
                return httpClient.execute(new HttpGet(downloadUrl), response -> {
                    if (isSuccessful(response.getCode())) {
                        var inputStream = response.getEntity().getContent();
                        // TODO: temporary hack. We need to redesign source resolver.
                        var charset = Encodings.resolveCharset(dcatDistribution.getEncoding())
                                .orElse(UTF_8);
                        var tmpString = IOUtils.toString(inputStream, charset);
                        var copyStream = IOUtils.toInputStream(tmpString, charset);
                        return Optional.of(copyStream);
                    } else {
                        throw new SourceResolverException(
                                String.format("Failed to download source%n%s", exception(dcatDistribution)));
                    }
                });
            } else {
                throw new SourceResolverException(String.format(
                        "Unsupported protocol %s for source%n%s", url.getProtocol(), exception(dcatDistribution)));
            }
        } catch (MalformedURLException | URISyntaxException urlException) {
            throw new SourceResolverException(
                    String.format(
                            "Encountered malformed URL %s for source%n%s", downloadUrl, exception(dcatDistribution)),
                    urlException);
        } catch (IOException ioException) {
            throw new SourceResolverException(
                    String.format("Failed to download source%n%s", exception(dcatDistribution)), ioException);
        }
    }

    private boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
