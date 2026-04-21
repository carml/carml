package io.carml.engine.target;

import io.carml.model.FilePath;
import io.carml.output.RdfSerializerFactory;
import io.carml.output.SerializerMode;
import io.carml.util.Encodings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;

/**
 * Creates {@link TargetWriter} instances from RML target model objects. Handles mapping of
 * serialization format IRIs ({@code http://www.w3.org/ns/formats/}) to the bare format tokens
 * understood by the {@link RdfSerializerFactory} SPI, resolution of file paths from
 * {@link FilePath} targets, and configuration of compression and encoding.
 *
 * <p>For file-based targets, use {@link #createFileWriter(FilePath, IRI, IRI, IRI)} to pass
 * serialization, encoding, and compression explicitly — this mirrors the RML-IO specification
 * where all three are primarily properties of {@code rml:LogicalTarget}.
 */
@Slf4j
@Builder
public class TargetWriterFactory {

    private static final String FORMATS_NS = "http://www.w3.org/ns/formats/";

    /**
     * Default {@link RdfSerializerFactory} shared by instances that do not configure one
     * explicitly. Initialized once at class load to avoid a {@link java.util.ServiceLoader} scan on
     * every builder invocation — mirrors the single-scan pattern used by
     * {@link FileTargetWriter}.
     */
    private static final RdfSerializerFactory DEFAULT_SERIALIZER_FACTORY = RdfSerializerFactory.create();

    /**
     * Maps W3C Formats namespace IRIs directly to the bare format tokens understood by the
     * {@link RdfSerializerFactory} SPI. Bare tokens align with common file extensions.
     *
     * <p>{@code formats:RDF_JSON} is intentionally omitted: no {@code RdfSerializerProvider}
     * currently registers an {@code "rdfjson"} token, so requests for that format are rejected at
     * configuration time by {@link #resolveSerializerFormat(IRI)} rather than failing mid-write
     * inside {@link RdfSerializerFactory#selectProvider}.
     */
    private static final Map<String, String> SERIALIZER_FORMAT_BY_IRI = Map.of(
            FORMATS_NS + "N-Triples", "nt",
            FORMATS_NS + "N-Quads", "nq",
            FORMATS_NS + "Turtle", "ttl",
            FORMATS_NS + "TriG", "trig",
            FORMATS_NS + "JSON-LD", "jsonld",
            FORMATS_NS + "RDF_XML", "rdfxml",
            FORMATS_NS + "N3", "n3");

    private static final String DEFAULT_FORMAT_TOKEN = "nq";

    /**
     * Base path for resolving relative file paths when root is
     * {@code rml:CurrentWorkingDirectory}. Defaults to the current working directory.
     */
    @Builder.Default
    private final Path basePath = Paths.get("").toAbsolutePath();

    /**
     * Path of the mapping file directory, used for resolving relative file paths when root is
     * {@code rml:MappingDirectory}.
     */
    private final Path mappingDirectoryPath;

    /**
     * {@link RdfSerializerFactory} used both for config-time provider validation and for creating
     * serializers inside the {@link FileTargetWriter}. Defaults to a class-static factory instance
     * shared across all builders — see {@link #DEFAULT_SERIALIZER_FACTORY}.
     */
    @Builder.Default
    private final RdfSerializerFactory serializerFactory = DEFAULT_SERIALIZER_FACTORY;

    /**
     * Creates a {@link FileTargetWriter} for a file-based RML target. Per the RML-IO specification,
     * {@code rml:serialization}, {@code rml:encoding}, and {@code rml:compression} are primarily
     * properties of {@code rml:LogicalTarget} — callers (e.g. {@code CarmlMapCommand.buildTargetRouter})
     * resolve effective values with LogicalTarget-level precedence and pass them here, rather than
     * reading them off the {@link FilePath} alone.
     *
     * @param filePath the file path target providing path and root (encoding/compression on the
     *     file-path are NOT consulted)
     * @param serialization the serialization format IRI, or {@code null} to use the default
     *     (N-Quads)
     * @param encoding the encoding IRI to apply, or {@code null} to use the writer default
     * @param compression the compression IRI to apply, or {@code null} for uncompressed output
     * @return a configured {@link FileTargetWriter}, not yet opened
     * @throws IllegalArgumentException if {@code serialization} is a non-null IRI that cannot be
     *     resolved, or if no registered provider supports the resolved token in streaming mode
     */
    public TargetWriter createFileWriter(FilePath filePath, IRI serialization, IRI encoding, IRI compression) {
        var formatToken = resolveSerializerFormat(serialization);
        validateProviderExists(formatToken);
        var charset = Encodings.resolveCharset(encoding).orElse(null);
        var resolvedPath = resolveFilePath(filePath);

        LOG.debug(
                "Creating file target writer: path={}, format={}, compression={}",
                resolvedPath,
                formatToken,
                compression);

        return FileTargetWriter.builder()
                .filePath(resolvedPath)
                .format(formatToken)
                .serializerFactory(serializerFactory)
                .compression(compression)
                .charset(charset)
                .build();
    }

    /**
     * Resolves a W3C Formats namespace IRI directly to the bare serializer format token expected by
     * {@link RdfSerializerFactory}.
     *
     * <p><strong>Behavior change vs. Task 7.7:</strong> previously, IRIs in the Formats namespace
     * that were not in the lookup table produced a warning and silently fell back to N-Quads. Now
     * any unknown IRI — whether in the Formats namespace (e.g. {@code formats:RDF_JSON}, or any
     * future addition we have not mapped) or outside it (not a valid {@code rml:serialization}
     * value) — causes an {@link IllegalArgumentException} to be thrown. Failing fast at
     * configuration time is the right call now that the {@link RdfSerializerFactory} SPI rejects
     * bad tokens at runtime anyway, and it surfaces configuration mistakes before any file is
     * opened.
     *
     * @param serialization the format IRI (e.g. {@code http://www.w3.org/ns/formats/N-Triples}),
     *     or {@code null} to use the default N-Quads token
     * @return the bare format token (e.g. {@code "nt"}, {@code "nq"}, {@code "ttl"})
     * @throws IllegalArgumentException if {@code serialization} is non-null and does not map to a
     *     supported format token
     */
    private static String resolveSerializerFormat(IRI serialization) {
        if (serialization == null) {
            return DEFAULT_FORMAT_TOKEN;
        }
        var iri = serialization.stringValue();
        var token = SERIALIZER_FORMAT_BY_IRI.get(iri);
        if (token != null) {
            return token;
        }
        if (iri.startsWith(FORMATS_NS)) {
            throw new IllegalArgumentException(
                    "Unsupported serialization format IRI <%s>: no serializer format token mapping. Supported IRIs: %s"
                            .formatted(iri, SERIALIZER_FORMAT_BY_IRI.keySet()));
        }
        throw new IllegalArgumentException(
                "Invalid serialization IRI <%s>: not in the W3C Formats namespace <%s>".formatted(iri, FORMATS_NS));
    }

    /**
     * Validates that the configured {@link RdfSerializerFactory} has a provider for the given
     * format token in {@link SerializerMode#STREAMING} mode. Surfaces misconfigurations (e.g.
     * using {@code carml-jar} without {@code carml-serializer-jena} on the classpath but
     * requesting Turtle) before any file is opened.
     *
     * @param formatToken the bare format token
     * @throws IllegalArgumentException if no registered provider supports the combination; the
     *     message lists the available provider class names for diagnostic context
     */
    private void validateProviderExists(String formatToken) {
        var hasProvider = serializerFactory.getProviders().stream()
                .anyMatch(provider -> provider.supports(formatToken, SerializerMode.STREAMING));
        if (!hasProvider) {
            var available = serializerFactory.getProviders().stream()
                    .map(provider -> provider.getClass().getSimpleName())
                    .toList();
            throw new IllegalArgumentException(
                    "No RdfSerializerProvider supports format token \"%s\" in STREAMING mode (available providers: %s)"
                            .formatted(formatToken, available));
        }
    }

    private Path resolveFilePath(FilePath filePath) {
        var relativePath = normalizeRelativePath(filePath.getPath());
        var root = filePath.getRoot();

        if (root != null && root.stringValue().endsWith("MappingDirectory")) {
            if (mappingDirectoryPath != null) {
                return mappingDirectoryPath.resolve(relativePath);
            }
            LOG.warn("MappingDirectory root specified but no mapping directory path configured, using base path");
        }

        return basePath.resolve(relativePath);
    }

    private static String normalizeRelativePath(String path) {
        if (path.startsWith("./")) {
            return path.substring(2);
        }
        if (path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }
}
