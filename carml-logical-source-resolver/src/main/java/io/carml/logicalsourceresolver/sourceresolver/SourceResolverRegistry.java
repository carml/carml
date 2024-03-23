package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.DcatDistribution;
import io.carml.model.FileSource;
import io.carml.model.RelativePathSource;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class SourceResolverRegistry {

    private static final SourceResolverRegistry INSTANCE = new SourceResolverRegistry(Map.of(
            String.class, FileResolver.getInstance(),
            FileSource.class, FileResolver.getInstance(),
            RelativePathSource.class, FileResolver.getInstance(),
            DcatDistribution.class, DcatDistributionResolver.of()));

    public static SourceResolverRegistry getInstance() {
        return INSTANCE;
    }

    private final Map<Class<?>, SourceResolver> sourceResolvers;

    public void addSourceResolver(Class<?> sourceClass, SourceResolver sourceResolver) {
        sourceResolvers.put(sourceClass, sourceResolver);
    }
}
