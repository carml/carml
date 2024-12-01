package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;

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
        Path path;
        if (basePath != null) {
            var relativePath = handleAsRelativePath(pathString);
            path = basePath.resolve(relativePath);
        } else if (classPathBase != null) {
            String sourceName = classPathBase.isEmpty()
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
        } else if (pathRelativeTo != null) {
            var relativePath = handleAsRelativePath(pathString);

            if (pathRelativeTo == PathRelativeTo.MAPPING_DIRECTORY) {
                var mappingDirPath = getMappingDirPath(source, mapping);
                path = mappingDirPath.resolve(relativePath);
            } else {
                // Current working directory
                path = Paths.get("").resolve(relativePath);
            }
        } else {
            var relativePath = handleAsRelativePath(pathString);
            path = Paths.get(relativePath);
        }

        return resolvePath(source, path);
    }

    private String handleAsRelativePath(String pathString) {
        return pathString.startsWith("/") ? pathString.replaceFirst("/", "") : pathString;
    }

    private String getClassPathBaseTemplate(String classPathBase) {
        return classPathBase.endsWith("/") ? "%s%s" : "%s/%s";
    }

    private Path getMappingDirPath(Source source, Mapping mapping) {
        if (mapping.getMappingFilePaths().isEmpty()) {
            throw new SourceResolverException(
                    String.format("No mapping file paths provided for source %s.", exception(source)));
        }

        var mappingDirs = mapping
                .getMappingFilePaths() //
                .stream()
                .map(Path::getParent)
                .toList();

        if (mappingDirs.size() > 1) {
            throw new SourceResolverException(String.format(
                    "Multiple mapping directories found, where only one was expected, for source %s.",
                    exception(source)));
        } else if (mappingDirs.isEmpty()) {
            // This means that mappings are in the root of the file system
            return Paths.get("/");
        } else {
            return mappingDirs.get(0);
        }
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
