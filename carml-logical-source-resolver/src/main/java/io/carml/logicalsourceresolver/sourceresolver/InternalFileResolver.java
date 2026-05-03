package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.FilePath;
import io.carml.model.Mapping;
import io.carml.model.Source;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.Builder;
import org.eclipse.rdf4j.model.IRI;
import reactor.core.publisher.Mono;

@Builder
class InternalFileResolver {

    private final URL url;

    private final Path basePath;

    private final String classPathBase;

    private final Class<?> loadingClass;

    private final String pathString;

    private final PathRelativeTo pathRelativeTo;

    private final IRI compression;

    Mono<InputStream> resolve(Source source, Mapping mapping) {
        Mono<InputStream> inputStreamMono;
        if (pathString != null) {
            inputStreamMono = handlePathString(source, mapping);
        } else if (url != null) {
            inputStreamMono = handleUrl(source);
        } else {
            throw new SourceResolverException(
                    String.format("No path or URL provided for source %s.", exception(source)));
        }

        if (compression != null) {
            return inputStreamMono.map(inputStream -> Compressions.decompress(inputStream, compression));
        } else {
            return inputStreamMono;
        }
    }

    private Mono<InputStream> handlePathString(Source source, Mapping mapping) {
        if (basePath != null) {
            return resolvePath(source, basePath.resolve(handleAsRelativePath(pathString)));
        }
        if (classPathBase != null) {
            return loadFromClasspath(source);
        }
        if (pathRelativeTo != null) {
            return resolvePath(source, resolveByAnchor(source, mapping));
        }
        return resolvePath(source, Paths.get(handleAsRelativePath(pathString)));
    }

    private Mono<InputStream> loadFromClasspath(Source source) {
        var sourceName = classPathBase.isEmpty()
                ? pathString
                : String.format(getClassPathBaseTemplate(classPathBase), classPathBase, pathString);

        var inputStream = loadingClass == null
                ? InternalFileResolver.class.getClassLoader().getResourceAsStream(sourceName)
                : loadingClass.getResourceAsStream(sourceName);

        if (inputStream == null) {
            throw new SourceResolverException(String.format(
                    "Could not resolve classpath resource %s for source%n%s.", sourceName, exception(source)));
        }

        return Mono.just(inputStream);
    }

    private Path resolveByAnchor(Source source, Mapping mapping) {
        // FilePath has rml:root semantics including absolute-path detection plus the dedup-WARN
        // for absolute-path-with-root. SourcePathResolver owns that logic so every caller honors
        // rml:root identically.
        if (source instanceof FilePath) {
            return SourcePathResolver.resolveAnchoredPath(source, mapping);
        }
        // Generic anchor resolution for any other aspect-providing source. Aspects decide WHERE to
        // anchor (PathRelativeTo); the resolver just applies it. Reference formulations such as
        // CsvwTable land here.
        var relativePath = handleAsRelativePath(pathString);
        if (pathRelativeTo == PathRelativeTo.MAPPING_DIRECTORY) {
            return SourcePathResolver.resolveMappingDirectory(source, mapping).resolve(relativePath);
        }
        return Paths.get("").resolve(relativePath);
    }

    private String handleAsRelativePath(String pathString) {
        return pathString.startsWith("/") ? pathString.replaceFirst("/", "") : pathString;
    }

    private String getClassPathBaseTemplate(String classPathBase) {
        return classPathBase.endsWith("/") ? "%s%s" : "%s/%s";
    }

    private Mono<InputStream> resolvePath(Source source, Path path) {
        if (Files.exists(path)) {
            return Mono.just(path).handle((path1, sink) -> {
                try {
                    sink.next(Files.newInputStream(path1));
                } catch (IOException ioException) {
                    sink.error(new SourceResolverException(
                            String.format("Could not resolve file path %s for source%n%s.", path1, exception(source)),
                            ioException));
                }
            });
        } else {
            throw new SourceResolverException(
                    String.format("File does not exist at path %s for source%n%s.", path, exception(source)));
        }
    }

    private Mono<InputStream> handleUrl(Source source) {
        try {
            if (url.getProtocol().equals("file")) {
                return resolvePath(source, Paths.get(url.toURI()));
            } else if (url.getProtocol().equals("http") || url.getProtocol().equals("https")) {
                return GetHttpUrl.getInstance().apply(url);
            } else {
                throw new SourceResolverException(
                        String.format("Unsupported protocol %s for source%n%s", url.getProtocol(), exception(source)));
            }
        } catch (URISyntaxException uriSyntaxException) {
            throw new SourceResolverException(
                    String.format("Encountered malformed URI %s for source%n%s", url, exception(source)),
                    uriSyntaxException);
        }
    }
}
