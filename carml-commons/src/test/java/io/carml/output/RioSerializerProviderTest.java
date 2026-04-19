package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class RioSerializerProviderTest {

    private final RioSerializerProvider provider = new RioSerializerProvider();

    // ---- supports() ----

    static Stream<Arguments> supportedCombinations() {
        return Stream.of(
                Arguments.of("nt", SerializerMode.STREAMING),
                Arguments.of("nt", SerializerMode.PRETTY),
                Arguments.of("ntriples", SerializerMode.STREAMING),
                Arguments.of("nq", SerializerMode.STREAMING),
                Arguments.of("nq", SerializerMode.PRETTY),
                Arguments.of("nquads", SerializerMode.STREAMING),
                Arguments.of("ttl", SerializerMode.STREAMING),
                Arguments.of("ttl", SerializerMode.PRETTY),
                Arguments.of("turtle", SerializerMode.PRETTY),
                Arguments.of("ttls", SerializerMode.STREAMING),
                Arguments.of("ttls", SerializerMode.PRETTY),
                Arguments.of("trig", SerializerMode.STREAMING),
                Arguments.of("trig", SerializerMode.PRETTY),
                Arguments.of("trigs", SerializerMode.STREAMING),
                Arguments.of("trigs", SerializerMode.PRETTY),
                Arguments.of("rdf", SerializerMode.PRETTY),
                Arguments.of("rdfxml", SerializerMode.PRETTY),
                Arguments.of("jsonld", SerializerMode.PRETTY),
                Arguments.of("jsonld", SerializerMode.STREAMING),
                Arguments.of("ndjsonld", SerializerMode.STREAMING),
                Arguments.of("ndjsonld", SerializerMode.PRETTY),
                Arguments.of("n3", SerializerMode.PRETTY),
                Arguments.of("trix", SerializerMode.STREAMING),
                Arguments.of("trix", SerializerMode.PRETTY),
                Arguments.of("brf", SerializerMode.STREAMING),
                Arguments.of("brf", SerializerMode.PRETTY),
                Arguments.of("rj", SerializerMode.STREAMING),
                Arguments.of("rj", SerializerMode.PRETTY));
    }

    @ParameterizedTest(name = "supports({0}, {1}) = true")
    @MethodSource("supportedCombinations")
    void supports_supportedCombinations_returnsTrue(String format, SerializerMode mode) {
        assertThat(provider.supports(format, mode), is(true));
    }

    @ParameterizedTest(name = "supports({0}, BYTE_LEVEL) = false")
    @ValueSource(
            strings = {
                "nt",
                "nq",
                "ttl",
                "ttls",
                "trig",
                "trigs",
                "rdf",
                "rdfxml",
                "jsonld",
                "ndjsonld",
                "n3",
                "trix",
                "brf",
                "rj"
            })
    void supports_byteLevelMode_returnsFalse(String format) {
        assertThat(provider.supports(format, SerializerMode.BYTE_LEVEL), is(false));
    }

    @Test
    void supports_unknownFormat_returnsFalse() {
        assertThat(provider.supports("bogus", SerializerMode.STREAMING), is(false));
        assertThat(provider.supports("bogus", SerializerMode.PRETTY), is(false));
    }

    @Test
    void supports_nullFormatOrMode_returnsFalse() {
        assertThat(provider.supports(null, SerializerMode.STREAMING), is(false));
        assertThat(provider.supports("ttl", null), is(false));
    }

    // ---- priority() ----

    @Test
    void priority_returns10() {
        assertThat(provider.priority(), is(10));
    }

    // ---- createSerializer() ----

    static Stream<Arguments> streamingFormats() {
        return Stream.of(
                Arguments.of("nt"),
                Arguments.of("ntriples"),
                Arguments.of("nq"),
                Arguments.of("nquads"),
                Arguments.of("ttl"),
                Arguments.of("turtle"),
                Arguments.of("ttls"),
                Arguments.of("trig"),
                Arguments.of("trigs"),
                Arguments.of("rdf"),
                Arguments.of("rdfxml"),
                Arguments.of("jsonld"),
                Arguments.of("ndjsonld"),
                Arguments.of("n3"),
                Arguments.of("trix"),
                Arguments.of("brf"),
                Arguments.of("rj"));
    }

    @ParameterizedTest(name = "createSerializer({0}, STREAMING) returns RioStreamingSerializer")
    @MethodSource("streamingFormats")
    void createSerializer_streamingModes_returnsStreamingSerializer(String format) {
        try (var serializer = provider.createSerializer(format, SerializerMode.STREAMING)) {
            assertThat(serializer, is(instanceOf(RioStreamingSerializer.class)));
        }
    }

    @ParameterizedTest(name = "createSerializer({0}, PRETTY) returns RioModelSerializer")
    @MethodSource("streamingFormats")
    void createSerializer_prettyModes_returnsModelSerializer(String format) {
        try (var serializer = provider.createSerializer(format, SerializerMode.PRETTY)) {
            assertThat(serializer, is(instanceOf(RioModelSerializer.class)));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_byteLevelMode_throwsIllegalArgumentException() {
        var thrown = assertThrows(
                IllegalArgumentException.class, () -> provider.createSerializer("nt", SerializerMode.BYTE_LEVEL));
        assertThat(thrown.getMessage(), is("Unsupported format/mode combination: nt/BYTE_LEVEL"));
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_unknownFormat_throwsIllegalArgumentException() {
        var thrown = assertThrows(
                IllegalArgumentException.class, () -> provider.createSerializer("bogus", SerializerMode.STREAMING));
        assertThat(thrown.getMessage(), is("Unsupported format/mode combination: bogus/STREAMING"));
    }

    // ---- ServiceLoader discovery ----

    @Test
    void serviceLoader_discoversRioSerializerProvider() {
        var providers = ServiceLoader.load(RdfSerializerProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(RioSerializerProvider.class::isInstance)
                .toList();

        assertThat(providers.size(), is(1));
    }

    @Test
    void serviceLoader_discoversBothFastAndRioProviders() {
        var providers = ServiceLoader.load(RdfSerializerProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .toList();

        var hasFast = providers.stream().anyMatch(FastSerializerProvider.class::isInstance);
        var hasRio = providers.stream().anyMatch(RioSerializerProvider.class::isInstance);

        assertThat(hasFast, is(true));
        assertThat(hasRio, is(true));
        assertThat(providers.size(), is(greaterThanOrEqualTo(2)));
    }
}
