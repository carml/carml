package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FastSerializerProviderTest {

    private final FastSerializerProvider provider = new FastSerializerProvider();

    // ---- supports() ----

    static Stream<Arguments> supportedCombinations() {
        return Stream.of(
                Arguments.of("nt", SerializerMode.STREAMING, true),
                Arguments.of("nt", SerializerMode.BYTE_LEVEL, true),
                Arguments.of("nq", SerializerMode.STREAMING, true),
                Arguments.of("nq", SerializerMode.BYTE_LEVEL, true));
    }

    @ParameterizedTest(name = "supports({0}, {1}) = {2}")
    @MethodSource("supportedCombinations")
    void supports_supportedCombinations_returnsTrue(String format, SerializerMode mode, boolean expected) {
        assertThat(provider.supports(format, mode), is(expected));
    }

    static Stream<Arguments> unsupportedCombinations() {
        return Stream.of(
                Arguments.of("nt", SerializerMode.PRETTY),
                Arguments.of("nq", SerializerMode.PRETTY),
                Arguments.of("ttl", SerializerMode.STREAMING),
                Arguments.of("trig", SerializerMode.STREAMING),
                Arguments.of("jsonld", SerializerMode.STREAMING),
                Arguments.of("rdf", SerializerMode.PRETTY));
    }

    @ParameterizedTest(name = "supports({0}, {1}) = false")
    @MethodSource("unsupportedCombinations")
    void supports_unsupportedCombinations_returnsFalse(String format, SerializerMode mode) {
        assertThat(provider.supports(format, mode), is(false));
    }

    // ---- priority() ----

    @Test
    void priority_returns100() {
        assertThat(provider.priority(), is(100));
    }

    // ---- createSerializer() ----

    @Test
    void createSerializer_nt_streaming_returnsFastNTriplesSerializer() {
        try (var serializer = provider.createSerializer("nt", SerializerMode.STREAMING)) {
            assertThat(serializer, is(instanceOf(FastNTriplesSerializer.class)));
        }
    }

    @Test
    void createSerializer_nt_byteLevel_returnsFastNTriplesSerializer() {
        try (var serializer = provider.createSerializer("nt", SerializerMode.BYTE_LEVEL)) {
            assertThat(serializer, is(instanceOf(FastNTriplesSerializer.class)));
        }
    }

    @Test
    void createSerializer_nq_streaming_returnsFastNQuadsSerializer() {
        try (var serializer = provider.createSerializer("nq", SerializerMode.STREAMING)) {
            assertThat(serializer, is(instanceOf(FastNQuadsSerializer.class)));
        }
    }

    @Test
    void createSerializer_nq_byteLevel_returnsFastNQuadsSerializer() {
        try (var serializer = provider.createSerializer("nq", SerializerMode.BYTE_LEVEL)) {
            assertThat(serializer, is(instanceOf(FastNQuadsSerializer.class)));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_unsupportedFormat_throwsIllegalArgumentException() {
        // The lambda is expected to throw; no serializer is returned to close.
        var thrown = assertThrows(
                IllegalArgumentException.class, () -> provider.createSerializer("ttl", SerializerMode.STREAMING));
        assertThat(thrown.getMessage(), is("Unsupported format/mode combination: ttl/STREAMING"));
    }

    @Test
    @SuppressWarnings("resource")
    void createSerializer_unsupportedMode_throwsIllegalArgumentException() {
        // The lambda is expected to throw; no serializer is returned to close.
        var thrown = assertThrows(
                IllegalArgumentException.class, () -> provider.createSerializer("nt", SerializerMode.PRETTY));
        assertThat(thrown.getMessage(), is("Unsupported format/mode combination: nt/PRETTY"));
    }

    // ---- ServiceLoader discovery ----

    @Test
    void serviceLoader_discoversFastSerializerProvider() {
        var providers = ServiceLoader.load(RdfSerializerProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(p -> p instanceof FastSerializerProvider)
                .toList();

        assertThat(providers.size(), is(1));
    }
}
