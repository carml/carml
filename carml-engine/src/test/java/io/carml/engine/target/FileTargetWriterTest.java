package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileTargetWriterTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final Statement TEST_STATEMENT = VF.createStatement(
            VF.createIRI("http://example.org/subject"), RDF.TYPE, VF.createIRI("http://example.org/Type"));

    @TempDir
    Path tempDir;

    @Test
    void open_write_close_writesNTriplesFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
        assertThat(content, containsString(RDF.TYPE.stringValue()));
        assertThat(content, containsString("<http://example.org/Type>"));
    }

    @Test
    void open_write_close_writesNQuadsFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nq");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NQUADS)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_write_close_writesTurtleFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.ttl");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.TURTLE)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("example.org"));
    }

    @Test
    void open_write_close_withGzipCompression_writesCompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt.gz");
        var gzipIri = VF.createIRI(RML_NS, "gzip");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .compression(gzipIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        try (InputStream is = new GZIPInputStream(Files.newInputStream(file))) {
            var content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content, containsString("<http://example.org/subject>"));
        }
    }

    @Test
    void open_write_close_withZipCompression_writesCompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt.zip");
        var zipIri = VF.createIRI(RML_NS, "zip");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .compression(zipIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        try (var zis = new ZipArchiveInputStream(Files.newInputStream(file))) {
            zis.getNextEntry();
            var content = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content, containsString("<http://example.org/subject>"));
        }
    }

    @Test
    void open_write_close_withCharset_writesEncodedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .charset(StandardCharsets.UTF_8)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_createsParentDirectories() throws IOException {
        // Given
        var file = tempDir.resolve("sub/dir/output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void flush_doesNotThrowAndDoesNotCorruptOutput() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
            writer.flush();
        }

        // Then - verify output is valid after flush + close
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_withNoneCompression_writesUncompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");
        var noneIri = VF.createIRI(RML_NS, "none");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .compression(noneIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void write_multipleStatements_allWrittenToFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        var statement1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/T1"));

        var statement2 = VF.createStatement(
                VF.createIRI("http://example.org/s2"), RDF.TYPE, VF.createIRI("http://example.org/T2"));

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .build()) {
            writer.open();
            writer.write(statement1);
            writer.write(statement2);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("http://example.org/s1"));
        assertThat(content, containsString("http://example.org/s2"));
    }

    @Test
    void open_invalidPath_throwsUncheckedIOException() {
        // Given
        var file = Path.of("/nonexistent/readonly/path/output.nt");

        // When/Then
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .rdfFormat(RDFFormat.NTRIPLES)
                .build()) {
            var exception = assertThrows(UncheckedIOException.class, writer::open);
            assertThat(exception.getMessage(), containsString("Failed to open file target"));
        }
    }
}
