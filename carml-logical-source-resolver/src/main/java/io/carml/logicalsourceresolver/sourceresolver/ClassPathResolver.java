package io.carml.logicalsourceresolver.sourceresolver;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(staticName = "of")
public class ClassPathResolver {

    private final String classPathBase;

    private final Class<?> loadingClass;

    public static ClassPathResolver of(String basePath) {
        return of(basePath, null);
    }

    public static ClassPathResolver of(Class<?> loadingClass) {
        return of("", loadingClass);
    }
}
