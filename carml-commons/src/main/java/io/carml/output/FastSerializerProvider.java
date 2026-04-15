package io.carml.output;

import com.google.auto.service.AutoService;
import java.util.Set;

/**
 * {@link RdfSerializerProvider} for the high-throughput Fast N-Triples and N-Quads serializers.
 *
 * <p>Supports {@link SerializerMode#STREAMING} and {@link SerializerMode#BYTE_LEVEL} modes for
 * the {@code "nt"} and {@code "nq"} formats. Registered at priority 100 (highest), so it is
 * preferred over Rio and Jena providers for these formats.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}.
 */
@AutoService(RdfSerializerProvider.class)
public final class FastSerializerProvider implements RdfSerializerProvider {

    private static final Set<String> SUPPORTED_FORMATS = Set.of("nt", "nq");

    private static final Set<SerializerMode> SUPPORTED_MODES =
            Set.of(SerializerMode.STREAMING, SerializerMode.BYTE_LEVEL);

    @Override
    public boolean supports(String format, SerializerMode mode) {
        return SUPPORTED_FORMATS.contains(format) && SUPPORTED_MODES.contains(mode);
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public RdfSerializer createSerializer(String format, SerializerMode mode) {
        if (!supports(format, mode)) {
            throw new IllegalArgumentException("Unsupported format/mode combination: %s/%s".formatted(format, mode));
        }
        return switch (format) {
            case "nt" -> FastNTriplesSerializer.withDefaults();
            case "nq" -> FastNQuadsSerializer.withDefaults();
            default -> throw new IllegalArgumentException("Unsupported format: %s".formatted(format));
        };
    }
}
