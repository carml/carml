package io.carml.model;

import io.carml.util.RmlMappingLoader;
import io.carml.util.RmlMappingLoaderException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;

@Getter
@Builder
public class Mapping {

    private static final RmlMappingLoader MAPPING_LOADER = RmlMappingLoader.build();

    @Singular
    private final List<String> classPathResources;

    @Singular
    private final List<Path> mappingFilePaths;

    @Singular
    private final Set<TriplesMap> triplesMaps;

    public static Mapping of(RDFFormat rdfFormat, String... classPathResources) {
        return of(rdfFormat, null, Arrays.stream(classPathResources).toList());
    }

    public static Mapping of(RDFFormat rdfFormat, Class<?> loadingClass, String... classPathResources) {
        return of(rdfFormat, loadingClass, Arrays.stream(classPathResources).toList());
    }

    public static Mapping of(RDFFormat rdfFormat, Class<?> loadingClass, List<String> classPathResources) {
        Function<String, URL> loadResource =
                loadingClass != null ? loadingClass::getResource : ClassLoader::getSystemResource;

        var paths = classPathResources.stream()
                .map(loadResource)
                .filter(Objects::nonNull)
                .map(url -> {
                    try {
                        return Path.of(url.toURI());
                    } catch (Exception e) {
                        throw new RmlMappingLoaderException(
                                String.format("Exception while load mapping from classpath resource %s", url), e);
                    }
                })
                .toList();

        if (paths.isEmpty()) {
            throw new RmlMappingLoaderException(
                    String.format("No mapping files found in classpath resources %s", classPathResources));
        }

        return of(rdfFormat, paths);
    }

    public static Mapping of(RDFFormat rdfFormat, List<Path> paths) {
        return new Mapping(List.of(), paths, MAPPING_LOADER.load(rdfFormat, paths.toArray(Path[]::new)));
    }

    public static Mapping of(RDFFormat rdfFormat, Path... paths) {
        var triplesMaps = MAPPING_LOADER.load(rdfFormat, paths);
        return new Mapping(List.of(), Arrays.stream(paths).toList(), triplesMaps);
    }

    public static Mapping of(RDFFormat rdfFormat, InputStream... inputStreams) {
        var triplesMaps = MAPPING_LOADER.load(rdfFormat, inputStreams);
        return new Mapping(List.of(), List.of(), triplesMaps);
    }

    public static Mapping of(Model... models) {
        var triplesMaps = MAPPING_LOADER.load(models);
        return new Mapping(List.of(), List.of(), triplesMaps);
    }
}
