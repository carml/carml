package io.carml.logicalsourceresolver.sourceresolver;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalsourceresolver.sourceresolver.aspects.FileSourceAspects;
import io.carml.model.Mapping;
import io.carml.model.Source;
import io.carml.util.TypeRef;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FileResolver implements SourceResolver<Mono<InputStream>> {

    private final Mapping mapping;

    private final Path basePath;

    private final String classPathBase;

    private final Class<?> loadingClass;

    private final List<FileSourceAspects> fileSourceAspects;

    public static FileResolverBuilder builder() {
        return new FileResolverBuilder();
    }

    @Override
    public boolean supportsSource(Source source) {
        return fileSourceAspects.stream().anyMatch(aspect -> aspect.supportsSource(source));
    }

    @Override
    public Optional<ResolvedSource<Mono<InputStream>>> apply(Source source) {
        var actionableAspects = fileSourceAspects.stream()
                .filter(aspects -> aspects.supportsSource(source))
                .toList();

        if (actionableAspects.isEmpty()) {
            return Optional.empty();
        }

        var resolverBuilder = InternalFileResolver.builder();

        handleAspect(source, actionableAspects, FileSourceAspects::getUrl, resolverBuilder::url, "URL");
        handleAspect(source, actionableAspects, FileSourceAspects::getBasePath, resolverBuilder::basePath, "base path");
        handleAspect(
                source,
                actionableAspects,
                FileSourceAspects::getPathString,
                resolverBuilder::pathString,
                "path string");
        handleAspect(
                source,
                actionableAspects,
                FileSourceAspects::getPathRelativeTo,
                resolverBuilder::pathRelativeTo,
                "path relative to");
        handleAspect(
                source,
                actionableAspects,
                FileSourceAspects::getCompression,
                resolverBuilder::compression,
                "compression");

        var resolved = resolverBuilder
                .basePath(basePath)
                .classPathBase(classPathBase)
                .loadingClass(loadingClass)
                .build()
                .resolve(source, mapping);

        return Optional.of(ResolvedSource.of(resolved, new TypeRef<>() {}));
    }

    private <T> void handleAspect(
            Source source,
            List<FileSourceAspects> aspectsList,
            Function<FileSourceAspects, Optional<Function<Source, Optional<T>>>> aspectGetter,
            Consumer<T> builderConsumer,
            String aspectName) {
        var aspectsByPriority = aspectsList.stream()
                .filter(aspect -> aspectGetter.apply(aspect).isPresent())
                .collect(groupingBy(FileSourceAspects::getPriority, toList()));

        aspectsByPriority.keySet().stream()
                .min(Integer::compare)
                .map(aspectsByPriority::get)
                .ifPresent(aspects -> {
                    if (aspects.size() > 1) {
                        throw new SourceResolverException(String.format(
                                "Multiple aspect resolvers for '%s' with the same priority.", aspectName));
                    }
                    aspectGetter
                            .apply(aspects.get(0))
                            .flatMap(urlFunction -> urlFunction.apply(source))
                            .ifPresent(builderConsumer);
                });
    }

    public static class FileResolverBuilder {
        private Mapping mapping;
        private Path basePath;
        private String classPathBase;
        private Class<?> loadingClass;

        FileResolverBuilder() {}

        public FileResolverBuilder mapping(Mapping mapping) {
            this.mapping = mapping;
            return this;
        }

        public FileResolverBuilder basePath(Path basePath) {
            this.basePath = basePath;
            return this;
        }

        public FileResolverBuilder classPathBase(String classPathBase) {
            this.classPathBase = classPathBase;
            return this;
        }

        public FileResolverBuilder loadingClass(Class<?> loadingClass) {
            this.loadingClass = loadingClass;
            return this;
        }

        public FileResolver build() {
            var fileSourceAspects = ServiceLoader.load(FileSourceAspects.class).stream()
                    .map(ServiceLoader.Provider::get)
                    .toList();

            return new FileResolver(mapping, basePath, classPathBase, loadingClass, fileSourceAspects);
        }
    }
}
