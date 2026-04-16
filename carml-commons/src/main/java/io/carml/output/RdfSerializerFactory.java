package io.carml.output;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/**
 * Factory for {@link RdfSerializer} instances that selects the best {@link RdfSerializerProvider}
 * for a requested format and {@link SerializerMode}.
 *
 * <p>Providers are discovered via {@link ServiceLoader} (by default using the current thread's
 * context class loader, falling back to this class's class loader) and cached in an immutable
 * list sorted by {@link RdfSerializerProvider#priority() priority} in descending order. When
 * multiple providers report the same priority, the relative order of the underlying
 * {@link ServiceLoader} iteration is preserved — a stable tiebreak determined by the JVM.
 *
 * <p>Selection semantics: {@link #selectProvider(String, SerializerMode)} returns the
 * highest-priority provider whose {@link RdfSerializerProvider#supports(String, SerializerMode)
 * supports} method returns {@code true}. This allows high-priority specialized providers (e.g.
 * {@link FastSerializerProvider} at priority 100 for {@code nt}/{@code nq}) to transparently
 * override lower-priority fallbacks (e.g. {@link RioSerializerProvider} at priority 10), while
 * still falling through to the fallback when the specialized provider does not support the
 * requested mode (for example, {@code nt}/{@link SerializerMode#PRETTY PRETTY} is rejected by
 * {@link FastSerializerProvider} and handled by {@link RioSerializerProvider} instead).
 *
 * <p><strong>Thread safety:</strong> instances are immutable after construction and therefore
 * safe to share across threads. The per-call {@link RdfSerializer} instances returned by
 * {@link #createSerializer(String, SerializerMode)} inherit the thread-safety characteristics
 * documented on {@link RdfSerializer}.
 *
 * <p><strong>Null-handling policy.</strong> Null {@code classLoader} or {@code providers}
 * arguments to the static factory methods throw {@link NullPointerException} per standard JDK
 * convention for mandatory constructor/factory dependencies. Null {@code format} or {@code mode}
 * arguments to the lookup methods ({@link #selectProvider(String, SerializerMode)},
 * {@link #createSerializer(String, SerializerMode)}) instead throw
 * {@link IllegalArgumentException} to match the underlying {@link RdfSerializerProvider}
 * null-handling contract — a null lookup key is treated as an invalid value rather than a
 * programming error.
 *
 * @see RdfSerializerProvider
 * @see SerializerMode
 */
public final class RdfSerializerFactory {

    private static final Comparator<RdfSerializerProvider> BY_PRIORITY_DESC =
            Comparator.comparingInt(RdfSerializerProvider::priority).reversed();

    private final List<RdfSerializerProvider> providers;

    private RdfSerializerFactory(List<RdfSerializerProvider> providers) {
        this.providers = providers;
    }

    /**
     * Creates a factory that discovers {@link RdfSerializerProvider}s via {@link ServiceLoader}
     * using the current thread's context class loader, or — if that class loader is {@code null}
     * — the class loader that loaded {@link RdfSerializerFactory}.
     *
     * @return a new factory instance
     */
    public static RdfSerializerFactory create() {
        var contextLoader = Thread.currentThread().getContextClassLoader();
        return create(contextLoader != null ? contextLoader : RdfSerializerFactory.class.getClassLoader());
    }

    /**
     * Creates a factory that discovers {@link RdfSerializerProvider}s via {@link ServiceLoader}
     * using the given class loader.
     *
     * @param classLoader the class loader used for SPI discovery; must not be {@code null}
     * @return a new factory instance
     * @throws NullPointerException if {@code classLoader} is {@code null}
     */
    public static RdfSerializerFactory create(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "classLoader must not be null");
        return of(ServiceLoader.load(RdfSerializerProvider.class, classLoader));
    }

    /**
     * Creates a factory backed by the given providers. Intended for tests and advanced use cases
     * where the set of providers is supplied explicitly instead of discovered via SPI. The
     * provided iterable is copied — later modifications do not affect the factory.
     *
     * @param providers the providers to use; must not be {@code null} and must not contain
     *     {@code null} entries
     * @return a new factory instance
     * @throws NullPointerException if {@code providers} is {@code null} or contains {@code null}
     */
    public static RdfSerializerFactory of(Iterable<RdfSerializerProvider> providers) {
        Objects.requireNonNull(providers, "providers must not be null");
        var sorted = StreamSupport.stream(providers.spliterator(), false)
                .map(provider -> Objects.requireNonNull(provider, "providers must not contain null entries"))
                .sorted(BY_PRIORITY_DESC)
                .toList();
        return new RdfSerializerFactory(sorted);
    }

    /**
     * Returns the list of providers known to this factory, sorted by priority in descending
     * order. The returned list is unmodifiable.
     *
     * @return the providers, never {@code null}
     */
    public List<RdfSerializerProvider> getProviders() {
        return providers;
    }

    /**
     * Returns the highest-priority provider that supports the given format and mode.
     *
     * @param format the RDF format identifier (file extension, e.g. {@code "nt"}, {@code "ttl"});
     *     must not be {@code null}
     * @param mode the requested serialization mode; must not be {@code null}
     * @return the selected provider, never {@code null}
     * @throws IllegalArgumentException if {@code format} or {@code mode} is {@code null}, or no
     *     registered provider supports the combination
     */
    public RdfSerializerProvider selectProvider(String format, SerializerMode mode) {
        rejectNullArgument(format, "format");
        rejectNullArgument(mode, "mode");
        return providers.stream()
                .filter(provider -> provider.supports(format, mode))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException("No RdfSerializerProvider supports %s/%s (available: %s)"
                                .formatted(
                                        format,
                                        mode,
                                        providers.stream()
                                                .map(provider ->
                                                        provider.getClass().getSimpleName())
                                                .toList())));
    }

    /**
     * Convenience method that {@linkplain #selectProvider(String, SerializerMode) selects} the
     * best provider for the given format and mode and creates a new {@link RdfSerializer}.
     *
     * @param format the RDF format identifier; must not be {@code null}
     * @param mode the requested serialization mode; must not be {@code null}
     * @return a new serializer instance, never {@code null}
     * @throws IllegalArgumentException if {@code format} or {@code mode} is {@code null}, or no
     *     registered provider supports the combination
     */
    public RdfSerializer createSerializer(String format, SerializerMode mode) {
        return selectProvider(format, mode).createSerializer(format, mode);
    }

    private static void rejectNullArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException("%s must not be null".formatted(name));
        }
    }
}
