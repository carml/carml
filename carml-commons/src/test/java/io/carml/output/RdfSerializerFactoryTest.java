package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RdfSerializerFactoryTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    // ---- ServiceLoader discovery ----

    @Test
    void create_discoversFastAndRioProvidersViaServiceLoader() {
        var factory = RdfSerializerFactory.create();

        var providers = factory.getProviders();

        assertThat(providers, hasItem(instanceOf(FastSerializerProvider.class)));
        assertThat(providers, hasItem(instanceOf(RioSerializerProvider.class)));
        assertThat(providers.size(), is(greaterThanOrEqualTo(2)));
    }

    @Test
    void create_withExplicitClassLoader_discoversProviders() {
        var factory = RdfSerializerFactory.create(RdfSerializerFactoryTest.class.getClassLoader());

        var providers = factory.getProviders();

        assertThat(providers, hasItem(instanceOf(FastSerializerProvider.class)));
        assertThat(providers, hasItem(instanceOf(RioSerializerProvider.class)));
    }

    @Test
    void create_withNullClassLoader_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> RdfSerializerFactory.create(null));
    }

    @Test
    void of_withNullIterable_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> RdfSerializerFactory.of(null));
    }

    @Test
    void of_withIterableContainingNull_throwsNullPointerException() {
        var providers = Arrays.<RdfSerializerProvider>asList(new FastSerializerProvider(), null);
        assertThrows(NullPointerException.class, () -> RdfSerializerFactory.of(providers));
    }

    // ---- getProviders() ----

    @Test
    void getProviders_returnsListSortedByPriorityDescending() {
        var factory = RdfSerializerFactory.of(List.of(new RioSerializerProvider(), new FastSerializerProvider()));

        assertThat(
                factory.getProviders(),
                contains(instanceOf(FastSerializerProvider.class), instanceOf(RioSerializerProvider.class)));
    }

    @Test
    void getProviders_returnsUnmodifiableList() {
        var factory = RdfSerializerFactory.of(List.of(new FastSerializerProvider(), new RioSerializerProvider()));
        var extraProvider = new FastSerializerProvider();

        var providers = factory.getProviders();
        assertThrows(UnsupportedOperationException.class, () -> providers.add(extraProvider));
        assertThrows(UnsupportedOperationException.class, () -> providers.remove(0));
    }

    // ---- Stable tiebreak for equal priorities ----

    @Test
    void of_equalPriorities_preservesInsertionOrderInGetProviders() {
        var first = new StubProvider(50, true);
        var second = new StubProvider(50, true);

        var factory = RdfSerializerFactory.of(List.of(first, second));

        assertThat(factory.getProviders(), contains(sameInstance(first), sameInstance(second)));
    }

    @Test
    void selectProvider_equalPriorities_returnsFirstInsertedSupportingProvider() {
        var first = new StubProvider(50, true);
        var second = new StubProvider(50, true);

        var factory = RdfSerializerFactory.of(List.of(first, second));

        assertThat(factory.selectProvider("nt", SerializerMode.STREAMING), is(sameInstance(first)));
    }

    // ---- Empty factory edge case ----

    @Test
    void of_emptyList_getProvidersIsEmptyAndUnmodifiable() {
        var factory = RdfSerializerFactory.of(List.of());
        var extraProvider = new FastSerializerProvider();

        var providers = factory.getProviders();
        assertThat(providers, is(empty()));
        assertThrows(UnsupportedOperationException.class, () -> providers.add(extraProvider));
    }

    @Test
    void of_emptyList_selectProviderThrowsIllegalArgumentException() {
        var factory = RdfSerializerFactory.of(List.of());

        var thrown = assertThrows(
                IllegalArgumentException.class, () -> factory.selectProvider("nt", SerializerMode.STREAMING));
        assertThat(thrown.getMessage(), containsString("No RdfSerializerProvider supports"));
        assertThat(thrown.getMessage(), containsString("nt"));
        assertThat(thrown.getMessage(), containsString("STREAMING"));
        assertThat(thrown.getMessage(), containsString("available: []"));
    }

    // ---- Defensive copy of source iterable ----

    @Test
    void of_defensiveCopy_mutationDoesNotAffectFactory() {
        var original = new StubProvider(50, true);
        var source = new ArrayList<RdfSerializerProvider>();
        source.add(original);

        var factory = RdfSerializerFactory.of(source);

        source.add(new StubProvider(75, true));
        source.remove(original);

        assertThat(factory.getProviders(), contains(sameInstance(original)));
        assertThat(factory.getProviders().size(), is(1));
    }

    // ---- selectProvider() priority routing (parameterized) ----

    static Stream<Arguments> selectProviderRoutingCases() {
        return Stream.of(
                arguments("nt", SerializerMode.STREAMING, FastSerializerProvider.class),
                arguments("nt", SerializerMode.BYTE_LEVEL, FastSerializerProvider.class),
                arguments("nq", SerializerMode.STREAMING, FastSerializerProvider.class),
                arguments("nq", SerializerMode.BYTE_LEVEL, FastSerializerProvider.class),
                arguments("nt", SerializerMode.PRETTY, RioSerializerProvider.class),
                arguments("ttl", SerializerMode.STREAMING, RioSerializerProvider.class),
                arguments("ttl", SerializerMode.PRETTY, RioSerializerProvider.class));
    }

    @ParameterizedTest(name = "selectProvider({0}, {1}) -> {2}")
    @MethodSource("selectProviderRoutingCases")
    void selectProvider_routesToExpectedProvider(
            String format, SerializerMode mode, Class<? extends RdfSerializerProvider> expected) {
        var factory = RdfSerializerFactory.create();

        var provider = factory.selectProvider(format, mode);

        assertThat(provider, is(instanceOf(expected)));
    }

    // ---- Custom high-priority provider overrides real providers ----

    @Test
    void selectProvider_withCustomHigherPriorityProvider_returnsCustomProvider() {
        var custom = new StubProvider(200, true);
        var factory =
                RdfSerializerFactory.of(List.of(new FastSerializerProvider(), new RioSerializerProvider(), custom));

        assertThat(factory.selectProvider("nt", SerializerMode.STREAMING), is(custom));
        assertThat(factory.selectProvider("ttl", SerializerMode.PRETTY), is(custom));
        assertThat(factory.selectProvider("nt", SerializerMode.BYTE_LEVEL), is(custom));
    }

    // ---- Error paths ----

    @Test
    void selectProvider_unsupportedFormat_throwsIllegalArgumentExceptionListingAvailableProviders() {
        var factory = RdfSerializerFactory.create();

        var thrown = assertThrows(
                IllegalArgumentException.class, () -> factory.selectProvider("bogus", SerializerMode.STREAMING));
        assertThat(thrown.getMessage(), containsString("bogus"));
        assertThat(thrown.getMessage(), containsString("STREAMING"));
        assertThat(thrown.getMessage(), containsString("FastSerializerProvider"));
        assertThat(thrown.getMessage(), containsString("RioSerializerProvider"));
    }

    @Test
    void selectProvider_nullFormat_throwsIllegalArgumentException() {
        var factory = RdfSerializerFactory.create();

        var thrown = assertThrows(
                IllegalArgumentException.class, () -> factory.selectProvider(null, SerializerMode.STREAMING));
        assertThat(thrown.getMessage(), containsString("format"));
    }

    @Test
    void selectProvider_nullMode_throwsIllegalArgumentException() {
        var factory = RdfSerializerFactory.create();

        var thrown = assertThrows(IllegalArgumentException.class, () -> factory.selectProvider("nt", null));
        assertThat(thrown.getMessage(), containsString("mode"));
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_unsupportedFormat_throwsIllegalArgumentException() {
        var factory = RdfSerializerFactory.create();

        assertThrows(IllegalArgumentException.class, () -> factory.createSerializer("bogus", SerializerMode.STREAMING));
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_nullFormat_throwsIllegalArgumentException() {
        var factory = RdfSerializerFactory.create();

        assertThrows(IllegalArgumentException.class, () -> factory.createSerializer(null, SerializerMode.STREAMING));
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_nullMode_throwsIllegalArgumentException() {
        var factory = RdfSerializerFactory.create();

        assertThrows(IllegalArgumentException.class, () -> factory.createSerializer("nt", null));
    }

    // ---- createSerializer() convenience — round-trip sanity ----

    @Test
    void createSerializer_ntStreaming_returnsFunctionalSerializer() {
        var factory = RdfSerializerFactory.create();
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = factory.createSerializer("nt", SerializerMode.STREAMING)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString();
        assertThat(result, containsString("<http://example.org/s>"));
        assertThat(result, containsString("<http://example.org/Thing>"));
    }

    @Test
    void createSerializer_ttlPretty_returnsFunctionalSerializer() {
        var factory = RdfSerializerFactory.create();
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = factory.createSerializer("ttl", SerializerMode.PRETTY)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString();
        assertThat(result, containsString("http://example.org/s"));
        assertThat(result, containsString("http://example.org/Thing"));
    }

    // ---- Test-only helper: a stub provider that always matches ----

    private record StubProvider(int priority, boolean supportsAll) implements RdfSerializerProvider {

        @Override
        public boolean supports(String format, SerializerMode mode) {
            return supportsAll;
        }

        @Override
        public RdfSerializer createSerializer(String format, SerializerMode mode) {
            throw new UnsupportedOperationException("stub");
        }
    }
}
