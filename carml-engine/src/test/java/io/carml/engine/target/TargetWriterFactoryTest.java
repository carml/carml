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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    void createFileWriter_ntriples_createsFileTargetWriter() {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);

        // Then
        assertThat(writer, instanceOf(FileTargetWriter.class));
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

        // Then
        var content = Files.readString(tempDir.resolve("output.ttl"));
        assertThat(content, containsString("example.org"));
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
    void createFileWriter_nullSerialization_defaultsToNQuads() throws IOException {
        // Given
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

        // Then
        var content = Files.readString(tempDir.resolve("output.nq"));
        assertThat(content, containsString("<http://example.org/s>"));
    }

    @Test
    void createFileWriter_unknownSerialization_defaultsToNQuads() throws IOException {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nq")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "UnknownFormat");
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
    void resolveRdfFormat_ntriples_returnsNTriples() {
        var iri = VF.createIRI(FORMATS_NS, "N-Triples");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.NTRIPLES));
    }

    @Test
    void resolveRdfFormat_nquads_returnsNQuads() {
        var iri = VF.createIRI(FORMATS_NS, "N-Quads");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.NQUADS));
    }

    @Test
    void resolveRdfFormat_turtle_returnsTurtle() {
        var iri = VF.createIRI(FORMATS_NS, "Turtle");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.TURTLE));
    }

    @Test
    void resolveRdfFormat_jsonld_returnsJsonLd() {
        var iri = VF.createIRI(FORMATS_NS, "JSON-LD");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.JSONLD));
    }

    @Test
    void resolveRdfFormat_rdfxml_returnsRdfXml() {
        var iri = VF.createIRI(FORMATS_NS, "RDF_XML");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.RDFXML));
    }

    @Test
    void resolveRdfFormat_null_returnsDefault() {
        assertThat(TargetWriterFactory.resolveRdfFormat(null), is(RDFFormat.NQUADS));
    }

    @Test
    void createFileWriter_withRdfJsonSerialization_throwsIllegalArgumentException() {
        // Given - RDF/JSON has no corresponding RdfSerializerProvider, so it must be rejected at
        // config time rather than failing mid-write inside RdfSerializerFactory.selectProvider
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
        assertThat(exception.getMessage(), containsString("RDF/JSON serialization is not supported"));
    }

    @Test
    void resolveRdfFormat_withTrigIri_returnsTrig() {
        var iri = VF.createIRI(FORMATS_NS, "TriG");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.TRIG));
    }

    @Test
    void resolveRdfFormat_withN3Iri_returnsN3() {
        var iri = VF.createIRI(FORMATS_NS, "N3");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.N3));
    }

    @Test
    void resolveRdfFormat_withRdfJsonIri_returnsRdfJson() {
        // The IRI-to-RDFFormat map still maps RDF_JSON; the format is only rejected later by the
        // private shim rdfFormatToSerializerFormat. Verifying the mapping separately keeps the two
        // responsibilities independently testable.
        var iri = VF.createIRI(FORMATS_NS, "RDF_JSON");
        assertThat(TargetWriterFactory.resolveRdfFormat(iri), is(RDFFormat.RDFJSON));
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
        // Given - JSON-LD exercises the rdfFormatToSerializerFormat shim end-to-end for the
        // "jsonld" token
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
        // Given - RDF/XML exercises the "rdfxml" token through the shim
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
