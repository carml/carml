package io.carml.output.jena;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.output.FastSerializerProvider;
import io.carml.output.RdfSerializerProvider;
import io.carml.output.RioSerializerProvider;
import io.carml.output.SerializerMode;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class JenaSerializerProviderTest {

    private final JenaSerializerProvider provider = new JenaSerializerProvider();

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
                Arguments.of("trig", SerializerMode.STREAMING),
                Arguments.of("trig", SerializerMode.PRETTY),
                Arguments.of("rdf", SerializerMode.PRETTY),
                Arguments.of("rdfxml", SerializerMode.PRETTY),
                Arguments.of("jsonld", SerializerMode.PRETTY),
                Arguments.of("n3", SerializerMode.PRETTY));
    }

    @ParameterizedTest(name = "supports({0}, {1}) = true")
    @MethodSource("supportedCombinations")
    void supports_supportedCombinations_returnsTrue(String format, SerializerMode mode) {
        assertThat(provider.supports(format, mode), is(true));
    }

    @ParameterizedTest(name = "supports({0}, BYTE_LEVEL) = false")
    @ValueSource(strings = {"nt", "nq", "ttl", "trig", "rdf", "rdfxml", "jsonld", "n3"})
    void supports_byteLevelMode_returnsFalse(String format) {
        assertThat(provider.supports(format, SerializerMode.BYTE_LEVEL), is(false));
    }

    @ParameterizedTest(name = "supports({0}, STREAMING) = false (pretty-only format)")
    @ValueSource(strings = {"rdf", "rdfxml", "jsonld", "n3"})
    void supports_prettyOnlyFormat_returnsFalseForStreaming(String format) {
        assertThat(provider.supports(format, SerializerMode.STREAMING), is(false));
    }

    @ParameterizedTest(name = "supports({0}, PRETTY) = true (pretty-only format)")
    @ValueSource(strings = {"rdf", "rdfxml", "jsonld", "n3"})
    void supports_prettyOnlyFormat_returnsTrueForPretty(String format) {
        assertThat(provider.supports(format, SerializerMode.PRETTY), is(true));
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
    void priority_returns50() {
        assertThat(provider.priority(), is(50));
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
                Arguments.of("trig"));
    }

    @ParameterizedTest(name = "createSerializer({0}, STREAMING) returns JenaStreamingSerializer")
    @MethodSource("streamingFormats")
    void createSerializer_streamingModes_returnsStreamingSerializer(String format) {
        try (var serializer = provider.createSerializer(format, SerializerMode.STREAMING)) {
            assertThat(serializer, is(instanceOf(JenaStreamingSerializer.class)));
        }
    }

    static Stream<Arguments> allFormats() {
        return Stream.of(
                Arguments.of("nt"),
                Arguments.of("ntriples"),
                Arguments.of("nq"),
                Arguments.of("nquads"),
                Arguments.of("ttl"),
                Arguments.of("turtle"),
                Arguments.of("trig"),
                Arguments.of("rdf"),
                Arguments.of("rdfxml"),
                Arguments.of("jsonld"),
                Arguments.of("n3"));
    }

    @ParameterizedTest(name = "createSerializer({0}, PRETTY) returns JenaModelSerializer")
    @MethodSource("allFormats")
    void createSerializer_prettyModes_returnsModelSerializer(String format) {
        try (var serializer = provider.createSerializer(format, SerializerMode.PRETTY)) {
            assertThat(serializer, is(instanceOf(JenaModelSerializer.class)));
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
    void serviceLoader_discoversJenaSerializerProvider() {
        var providers = ServiceLoader.load(RdfSerializerProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(JenaSerializerProvider.class::isInstance)
                .toList();

        assertThat(providers.size(), is(1));
    }

    @Test
    void serviceLoader_discoversJenaFastAndRioProviders() {
        var providers = ServiceLoader.load(RdfSerializerProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .toList();

        assertThat(providers.stream().anyMatch(JenaSerializerProvider.class::isInstance), is(true));
        assertThat(providers.stream().anyMatch(FastSerializerProvider.class::isInstance), is(true));
        assertThat(providers.stream().anyMatch(RioSerializerProvider.class::isInstance), is(true));
    }
}
