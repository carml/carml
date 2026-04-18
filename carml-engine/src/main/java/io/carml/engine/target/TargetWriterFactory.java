package io.carml.engine.target;

import io.carml.model.FilePath;
import io.carml.output.RdfSerializerFactory;
import io.carml.util.Encodings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.rio.RDFFormat;

/**
 * Creates {@link TargetWriter} instances from RML target model objects. Handles mapping of
 * serialization format IRIs ({@code http://www.w3.org/ns/formats/}) to RDF4J {@link RDFFormat},
 * resolution of file paths from {@link FilePath} targets, and configuration of compression and
 * encoding.
 *
 * <p>For file-based targets, use {@link #createFileWriter(FilePath, IRI)}. The serialization format
 * is passed separately because in RML-IO it is a property of the {@code rml:LogicalTarget}, not of
 * the target resource itself.
 */
@Slf4j
@Builder
public class TargetWriterFactory {

    private static final String FORMATS_NS = "http://www.w3.org/ns/formats/";

    private static final Map<String, RDFFormat> FORMAT_MAP = Map.of(
            FORMATS_NS + "N-Triples", RDFFormat.NTRIPLES,
            FORMATS_NS + "N-Quads", RDFFormat.NQUADS,
            FORMATS_NS + "Turtle", RDFFormat.TURTLE,
            FORMATS_NS + "TriG", RDFFormat.TRIG,
            FORMATS_NS + "JSON-LD", RDFFormat.JSONLD,
            FORMATS_NS + "RDF_XML", RDFFormat.RDFXML,
            FORMATS_NS + "N3", RDFFormat.N3,
            FORMATS_NS + "RDF_JSON", RDFFormat.RDFJSON);

    private static final RDFFormat DEFAULT_FORMAT = RDFFormat.NQUADS;

    /**
     * Maps RDF4J {@link RDFFormat}s to the bare format tokens understood by the
     * {@link io.carml.output.RdfSerializerFactory RdfSerializerFactory} SPI. Bare tokens align with
     * common file extensions.
     *
     * <p>{@link RDFFormat#RDFJSON} is intentionally absent: no {@code RdfSerializerProvider}
     * currently registers the {@code "rdfjson"} token, so attempting to select a serializer for
     * that format would fail at runtime inside {@link RdfSerializerFactory#selectProvider}. Config
     * time rejection via {@link #rdfFormatToSerializerFormat(RDFFormat)} surfaces the problem
     * before any file is opened.
     */
    private static final Map<RDFFormat, String> SERIALIZER_FORMAT_BY_RDF_FORMAT = Map.of(
            RDFFormat.NTRIPLES, "nt",
            RDFFormat.NQUADS, "nq",
            RDFFormat.TURTLE, "ttl",
            RDFFormat.TRIG, "trig",
            RDFFormat.JSONLD, "jsonld",
            RDFFormat.RDFXML, "rdfxml",
            RDFFormat.N3, "n3");

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
     * Creates a {@link FileTargetWriter} for a file-based RML target.
     *
     * @param filePath the file path target containing path, root, compression, and encoding
     * @param serialization the serialization format IRI (e.g. {@code formats:N-Triples}), or
     *     {@code null} to use the default (N-Quads)
     * @return a configured {@link FileTargetWriter}, not yet opened
     */
    public TargetWriter createFileWriter(FilePath filePath, IRI serialization) {
        var rdfFormat = resolveRdfFormat(serialization);
        var formatToken = rdfFormatToSerializerFormat(rdfFormat);
        var compression = filePath.getCompression();
        var charset = Encodings.resolveCharset(filePath.getEncoding()).orElse(null);
        var resolvedPath = resolveFilePath(filePath);

        LOG.debug(
                "Creating file target writer: path={}, format={}, compression={}",
                resolvedPath,
                rdfFormat,
                compression);

        return FileTargetWriter.builder()
                .filePath(resolvedPath)
                .format(formatToken)
                .compression(compression)
                .charset(charset)
                .build();
    }

    /**
     * Maps an RDF4J {@link RDFFormat} to the bare serializer format token expected by
     * {@link io.carml.output.RdfSerializerFactory}. Used here as a temporary shim until Task 7.8
     * reworks the factory to operate directly on serialization IRIs / format tokens.
     *
     * <p>{@link RDFFormat#RDFJSON} is rejected explicitly: no {@code RdfSerializerProvider}
     * registers the {@code "rdfjson"} token, and failing at config time is preferable to failing
     * mid-write inside {@link RdfSerializerFactory#selectProvider}.
     *
     * @throws IllegalArgumentException if {@code format} has no serializer token mapping
     *     (including {@link RDFFormat#RDFJSON})
     */
    private static String rdfFormatToSerializerFormat(RDFFormat format) {
        Objects.requireNonNull(format, "format must not be null");
        if (RDFFormat.RDFJSON.equals(format)) {
            throw new IllegalArgumentException("RDF/JSON serialization is not supported for file targets");
        }
        var token = SERIALIZER_FORMAT_BY_RDF_FORMAT.get(format);
        if (token == null) {
            throw new IllegalArgumentException("No serializer format token mapping for %s".formatted(format));
        }
        return token;
    }

    /**
     * Resolves a W3C Formats namespace IRI to the corresponding RDF4J {@link RDFFormat}.
     *
     * @param serialization the format IRI (e.g. {@code http://www.w3.org/ns/formats/N-Triples})
     * @return the corresponding {@link RDFFormat}, or {@link RDFFormat#NQUADS} as default
     */
    public static RDFFormat resolveRdfFormat(IRI serialization) {
        if (serialization == null) {
            return DEFAULT_FORMAT;
        }
        return Optional.ofNullable(FORMAT_MAP.get(serialization.stringValue())).orElseGet(() -> {
            LOG.warn("Unknown serialization format {}, defaulting to {}", serialization, DEFAULT_FORMAT);
            return DEFAULT_FORMAT;
        });
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
