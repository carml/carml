package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.carml.model.impl.CarmlFilePath;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
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
    void createFileWriter_withCompression_createsCompressedWriter() {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nt.gz")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .compression(VF.createIRI(RML_NS, "gzip"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);

        // Then
        assertThat(writer, instanceOf(FileTargetWriter.class));
    }

    @Test
    void createFileWriter_withEncoding_createsEncodedWriter() {
        // Given
        var filePath = CarmlFilePath.builder()
                .path("output.nt")
                .root(VF.createIRI(RML_NS, "CurrentWorkingDirectory"))
                .encoding(VF.createIRI(RML_NS, "UTF-8"))
                .build();
        var serialization = VF.createIRI(FORMATS_NS, "N-Triples");
        var factory = TargetWriterFactory.builder().basePath(tempDir).build();

        // When
        var writer = factory.createFileWriter(filePath, serialization);

        // Then
        assertThat(writer, instanceOf(FileTargetWriter.class));
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
}
