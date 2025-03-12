package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.Source;
import io.netty.handler.codec.http.HttpMethod;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.function.Function;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

@NoArgsConstructor(staticName = "getInstance")
public class GetHttpUrl implements Function<URL, Mono<InputStream>> {

    public static URL toUrl(String urlString) {
        return toUrl(urlString, null);
    }

    public static URL toUrl(String urlString, Source source) {
        String errorMessage = source != null
                ? String.format("Encountered malformed URL %s for source%n%s", urlString, exception(source))
                : String.format("Encountered malformed URL %s", urlString);
        return toUrlWithErrorMessage(urlString, errorMessage);
    }

    private static URL toUrlWithErrorMessage(String urlString, String errorMessage) {
        try {
            return new URL(urlString);
        } catch (MalformedURLException malformedUrlException) {
            throw new SourceResolverException(errorMessage, malformedUrlException);
        }
    }

    @Override
    public Mono<InputStream> apply(URL url) {
        try {
            return HttpClient.create()
                    .followRedirect(true)
                    .request(HttpMethod.GET)
                    .uri(url.toURI())
                    .responseContent()
                    .asInputStream()
                    .reduce(SequenceInputStream::new)
                    .doOnError(throwable -> {
                        throw new SourceResolverException(String.format("Failed to get URL %s", url), throwable);
                    });
        } catch (URISyntaxException uriSyntaxException) {
            throw new SourceResolverException(String.format("Encountered invalid URI %s", url), uriSyntaxException);
        }
    }
}
