package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.carml.model.impl.CarmlFilePath;
import io.carml.output.RdfSerializerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetWriterFactoryTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String FORMATS_NS = "http://www.w3.org/ns/formats/";

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final Statement TEST_STATEMENT =
            VF.createStatement(VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/T"));

    @TempDir
    Path tempDir;

    @Test
    void createFileWriter_ntriples_writesNTriplesOutput() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then
        assertThat(writer, instanceOf(FileTargetWriter.class));
        var content = Files.readString(tempDir.resolve("output.nt"));
        assertThat(content, containsString("<http://example.org/s>"));
        assertThat(content, containsString(RDF.TYPE.stringValue()));
    }

    @Test
    void createFileWriter_withNullRoot_resolvesAgainstBasePath() throws IOException {
        // Given - FilePath without an explicit rml:root must still resolve under basePath
        var filePath = CarmlFilePath.builder().path("output.nt").build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then
        var content = Files.readString(tempDir.resolve("output.nt"));
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_nquads_writesNQuadsOutput() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nq")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Quads");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then
        var content = Files.readString(tempDir.resolve("output.nq"));
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_turtle_writesTurtleOutput() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.ttl")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "Turtle");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - Rio can parse the output as Turtle and recovers the written statement
        var content = Files.readString(tempDir.resolve("output.ttl"));
        try (var input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            var parsedModel = Rio.parse(input, "", RDFFormat.TURTLE);
            assertThat(parsedModel.size(), equalTo(1));
            assertThat(parsedModel.contains(TEST_STATEMENT), is(true));
        }
    }

    @Test
    void createFileWriter_dotSlashPath_normalizesPath() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("./output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then
        var content = Files.readString(tempDir.resolve("output.nt"));
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_mappingDirectory_resolvesRelativeToMappingDir() throws IOException {
        // Given
        var mappingDir = tempDir.resolve("mappings");
        Files.createDirectories(mappingDir);

        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "MappingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder()
                .basePath(tempDir)
                .mappingDirectoryPath(mappingDir)
                .build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then
        var content = Files.readString(mappingDir.resolve("output.nt"));
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_withCompression_createsCompressedWriter() throws IOException {
        // Given - GZIP compression must be propagated from FilePath through the factory to the
        // FileTargetWriter, and the resulting bytes must decode correctly through GZIPInputStream
        var filePath = CarmlFilePath.builder()
                .path("output.nt.gz")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .compression(VF.createIRI(RML_NS, "gzip"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When - write a statement through the compressed writer
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - file exists and bytes decode through GZIP to the expected N-Triples content
        var compressedFile = tempDir.resolve("output.nt.gz");
        assertTrue(Files.exists(compressedFile));
        try (InputStream input = new GZIPInputStream(Files.newInputStream(compressedFile))) {
            var content = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content, containsString("<http://example.org/s>"));
            assertThat(content, containsString(RDF.TYPE.stringValue()));
        }
    }

    @Test
    void createFileWriter_withEncoding_createsEncodedWriter() throws IOException {
        // Given - UTF-16 encoding must be propagated from FilePath through the factory, and the
        // resulting bytes must decode correctly when read as UTF-16 (and NOT as UTF-8)
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .encoding(VF.createIRI(RML_NS, "UTF-16"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - reading with UTF-16 recovers the RDF content
        var outputFile = tempDir.resolve("output.nt");
        var utf16Content = Files.readString(outputFile, StandardCharsets.UTF_16);
        assertThat(utf16Content, containsString("<http://example.org/s>"));

        // And - raw bytes differ from the UTF-8 encoding of the same string, proving the charset
        // wrapper was actually applied (UTF-16 uses 2+ bytes per code unit plus a BOM)
        var rawBytes = Files.readAllBytes(outputFile);
        var utf8Bytes = utf16Content.getBytes(StandardCharsets.UTF_8);
        assertThat(rawBytes.length, not(equalTo(utf8Bytes.length)));
    }

    @Test
    void createFileWriter_withNullSerialization_defaultsToNQuads() throws IOException {
        // Given - explicit null serialization must resolve to the N-Quads default; verified by
        // writing a statement and reading the output back as N-Quads. The test doubles as coverage
        // for the null branch of the private resolveSerializerFormat method
        var filePath = CarmlFilePath.builder()
                .path("output.nq")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, null);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - the file parses as N-Quads and recovers the statement
        var outputFile = tempDir.resolve("output.nq");
        assertTrue(Files.exists(outputFile));
        try (var input = Files.newInputStream(outputFile)) {
            var parsedModel = Rio.parse(input, "", RDFFormat.NQUADS);
            assertThat(parsedModel.size(), is(1));
        }
    }

    @Test
    void createFileWriter_withUnknownFormatsIri_throwsIllegalArgumentException() {
        // Given - a W3C Formats namespace IRI that is not in the lookup table (e.g. a hypothetical
        // future format). Task 7.8 changed the behavior here: previously this warned and silently
        // fell back to N-Quads; now it must fail fast at config time
        var filePath = CarmlFilePath.builder()
                .path("output.nq")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "UnknownFormat");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When / Then
        var exception = assertThrows(IllegalArgumentException.class, () -> {
            try (var writer = factory.createFileWriter(filePath, serialization)) {
                fail("Expected IllegalArgumentException but createFileWriter returned: " + writer);
            }
        });
        assertThat(exception.getMessage(), containsString(FORMATS_NS + "UnknownFormat"));
        assertThat(exception.getMessage(), containsString("no serializer format token mapping"));
    }

    @Test
    void createFileWriter_withRdfJsonSerialization_throwsIllegalArgumentException() {
        // Given - RDF/JSON has no corresponding RdfSerializerProvider. Task 7.8 removed the
        // RDFFormat-based shim; rejection now happens in the IRI -> token resolver (same message
        // shape as any other unsupported Formats IRI), rather than in a format-specific shim
        var filePath = CarmlFilePath.builder()
                .path("output.rj")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "RDF_JSON");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When / Then
        var exception = assertThrows(IllegalArgumentException.class, () -> {
            try (var writer = factory.createFileWriter(filePath, serialization)) {
                fail("Expected IllegalArgumentException but createFileWriter returned: " + writer);
            }
        });
        assertThat(exception.getMessage(), containsString(FORMATS_NS + "RDF_JSON"));
        assertThat(exception.getMessage(), containsString("no serializer format token mapping"));
    }

    @Test
    void createFileWriter_withNonFormatsIri_throwsIllegalArgumentException() {
        // Given - an IRI outside the W3C Formats namespace is not a valid rml:serialization value.
        // The resolver rejects it with a distinct message pointing at the namespace requirement
        var filePath = CarmlFilePath.builder()
                .path("output.nq")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI("http://example.org/SomeFormat");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When / Then
        var exception = assertThrows(IllegalArgumentException.class, () -> {
            try (var writer = factory.createFileWriter(filePath, serialization)) {
                fail("Expected IllegalArgumentException but createFileWriter returned: " + writer);
            }
        });
        assertThat(exception.getMessage(), containsString("http://example.org/SomeFormat"));
        assertThat(exception.getMessage(), containsString("W3C Formats namespace"));
    }

    @Test
    void createFileWriter_withMissingProvider_throwsIllegalArgumentException() {
        // Given - a TargetWriterFactory configured with an RdfSerializerFactory that has no
        // providers. Even for a valid IRI like formats:N-Triples, config-time validation must fail
        // with a diagnostic message listing the (empty) set of available providers. This models
        // the "carml-jar without a serializer module on the classpath" misconfiguration
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var emptyFactory = RdfSerializerFactory.of(List.of());
        var factory = TargetWriterFactory.builder()
                .basePath(tempDir)
                .serializerFactory(emptyFactory)
                .build();

        // When / Then
        var exception = assertThrows(IllegalArgumentException.class, () -> {
            try (var writer = factory.createFileWriter(filePath, serialization)) {
                fail("Expected IllegalArgumentException but createFileWriter returned: " + writer);
            }
        });
        assertThat(exception.getMessage(), containsString("No RdfSerializerProvider"));
        assertThat(exception.getMessage(), containsString("nt"));
        assertThat(exception.getMessage(), containsString("STREAMING"));
    }

    @Test
    void createFileWriter_withTrigSerialization_writesTrigFile() throws IOException {
        // Given - a quad with a non-default graph context so the TriG output distinguishes itself
        // from Turtle via the graph block syntax
        var graph = VF.createIRI("http://example.org/g1");
        var quad = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/T"), graph);

        var filePath = CarmlFilePath.builder()
                .path("output.trig")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "TriG");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(quad);
        writer.close();

        // Then - the file contains the named-graph block that is specific to TriG
        var content = Files.readString(tempDir.resolve("output.trig"));
        assertThat(content, containsString("<http://example.org/g1>"));
        assertThat(content, containsString("{"));
        assertThat(content, containsString("}"));

        // And - Rio can parse the output as TriG without error
        try (var input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            var parsedModel = Rio.parse(input, "", RDFFormat.TRIG);
            assertThat(parsedModel.size(), is(1));
        }
    }

    @Test
    void createFileWriter_withN3Serialization_writesN3File() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.n3")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N3");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - the file exists and Rio parses it as N3 (Rio accepts N3 via its Turtle parser
        // compatibility) recovering the original statement
        var outputFile = tempDir.resolve("output.n3");
        assertTrue(Files.exists(outputFile));
        try (var input = Files.newInputStream(outputFile)) {
            var parsedModel = Rio.parse(input, "", RDFFormat.N3);
            assertThat(parsedModel.size(), is(1));
        }
    }

    @Test
    void createFileWriter_withJsonLdSerialization_writesJsonLdFile() throws IOException {
        // Given - JSON-LD exercises the IRI -> token resolver end-to-end for the "jsonld" token
        var filePath = CarmlFilePath.builder()
                .path("output.jsonld")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "JSON-LD");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - Rio parses the output as JSON-LD, confirming the format token was wired up
        var outputFile = tempDir.resolve("output.jsonld");
        assertTrue(Files.exists(outputFile));
        try (var input = Files.newInputStream(outputFile)) {
            var parsedModel = Rio.parse(input, "", RDFFormat.JSONLD);
            assertThat(parsedModel.size(), is(1));
        }
    }

    @Test
    void createFileWriter_withRdfXmlSerialization_writesRdfXmlFile() throws IOException {
        // Given - RDF/XML exercises the "rdfxml" token through the IRI -> token resolver
        var filePath = CarmlFilePath.builder()
                .path("output.rdf")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "RDF_XML");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - Rio parses the output as RDF/XML
        var outputFile = tempDir.resolve("output.rdf");
        assertTrue(Files.exists(outputFile));
        try (var input = Files.newInputStream(outputFile)) {
            var parsedModel = Rio.parse(input, "", RDFFormat.RDFXML);
            assertThat(parsedModel.size(), is(1));
        }
    }

    @Test
    void createFileWriter_withMappingDirectoryRootButNoMappingPath_fallsBackToBasePath() throws IOException {
        // Given - the FilePath declares MappingDirectory root, but the factory has no mapping
        // directory configured. This must trigger the warn-and-fallback branch in resolveFilePath,
        // where the file is resolved under basePath instead
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "MappingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - the file was written under basePath, confirming the fallback branch
        var outputFile = tempDir.resolve("output.nt");
        assertTrue(Files.exists(outputFile));
        var content = Files.readString(outputFile);
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_withAbsolutePathPrefix_stripsLeadingSlash() throws IOException {
        // Given - a path with a leading "/" must be normalized by stripping the leading slash so
        // that the resulting path resolves relatively against basePath rather than escaping to the
        // filesystem root
        var filePath = CarmlFilePath.builder()
                .path("/output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // Then - the file was written relative to basePath (tempDir/output.nt), not to /output.nt
        var outputFile = tempDir.resolve("output.nt");
        assertTrue(Files.exists(outputFile));
        var content = Files.readString(outputFile);
        assertThat(content, containsString("<http://example.org/s>"));
    }
}
