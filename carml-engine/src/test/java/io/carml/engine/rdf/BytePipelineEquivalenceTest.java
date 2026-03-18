package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Models.isomorphic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import io.carml.output.FastNTriplesSerializer;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Integration test that verifies the statement-less byte pipeline produces RDF output that is
 * semantically identical to the {@link org.eclipse.rdf4j.model.Statement}-based pipeline. Both
 * paths are exercised against the same RML mapping and CSV data, and the resulting models are
 * compared for isomorphism.
 */
class BytePipelineEquivalenceTest {

    private static final String CSV_DATA = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n";

    private static final String RML_MAPPING = """
            @prefix rml: <http://w3id.org/rml/> .
            @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            <#PersonMapping> a rml:TriplesMap ;
              rml:logicalSource [
                a rml:LogicalSource ;
                rml:source [
                  a rml:RelativePathSource ;
                  rml:root rml:MappingDirectory ;
                  rml:path "data.csv" ;
                ] ;
                rml:referenceFormulation ql:CSV ;
              ] ;
              rml:subjectMap [
                rml:template "http://example.org/person/{id}" ;
                rml:class ex:Person ;
              ] ;
              rml:predicateObjectMap [
                rml:predicate ex:name ;
                rml:objectMap [ rml:reference "name" ]
              ] ;
              rml:predicateObjectMap [
                rml:predicate ex:age ;
                rml:objectMap [ rml:reference "age" ; rml:datatype xsd:integer ]
              ] .
            """;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        Files.writeString(tempDir.resolve("data.csv"), CSV_DATA, StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("mapping.rml.ttl"), RML_MAPPING, StandardCharsets.UTF_8);
    }

    @Test
    void mapToNTriplesBytes_producesModelIsomorphicToStatementPath() throws IOException {
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, tempDir.resolve("mapping.rml.ttl"));

        // Statement path: map() -> Statements -> FastNTriplesSerializer -> parse back
        var statementMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .fileResolver(tempDir)
                .build();

        var statementOutput = new ByteArrayOutputStream();
        var serializer = FastNTriplesSerializer.withDefaults();
        Flux<org.eclipse.rdf4j.model.Statement> statements = statementMapper.map();
        serializer.serialize(statements, statementOutput);
        var statementModel = parseNTriples(statementOutput.toByteArray());

        // Byte path: mapToNTriplesBytes() -> concatenate -> parse
        var byteMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .fileResolver(tempDir)
                .build();

        var byteOutput = new ByteArrayOutputStream();
        byteMapper.mapToNTriplesBytes().toStream().forEach(bytes -> {
            try {
                byteOutput.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        var byteModel = parseNTriples(byteOutput.toByteArray());

        // Sanity check: both models should have the expected number of triples
        // 3 persons x (1 rdf:type + 1 ex:name + 1 ex:age) = 9 triples
        assertThat(statementModel.size(), is(9));
        assertThat(byteModel.size(), is(9));

        // Core assertion: the two models must be isomorphic
        assertThat(
                "Byte pipeline output must be isomorphic to Statement pipeline output",
                isomorphic(byteModel, statementModel),
                is(true));
    }

    @Test
    void mapToNQuadsBytes_producesValidNQuads() throws IOException {
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, tempDir.resolve("mapping.rml.ttl"));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .fileResolver(tempDir)
                .build();

        var output = new ByteArrayOutputStream();
        mapper.mapToNQuadsBytes().toStream().forEach(bytes -> {
            try {
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        var nquadBytes = output.toByteArray();
        assertThat("NQuads output should not be empty", nquadBytes.length, greaterThan(0));

        // For a mapping with no graph maps, N-Quads output has no graph field per line,
        // which means it is valid N-Triples too. Parse as N-Quads to verify validity.
        var nquadModel = parseNQuads(nquadBytes);

        // Also parse as N-Triples (no-graph lines are valid NT)
        var ntModel = parseNTriples(nquadBytes);

        assertThat(nquadModel.size(), is(9));
        assertThat(
                "N-Quads and N-Triples parse should produce isomorphic models for graph-less mapping",
                isomorphic(nquadModel, ntModel),
                is(true));
    }

    @Test
    void mapToNTriplesBytes_producesRoundTrippableOutput() throws IOException {
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, tempDir.resolve("mapping.rml.ttl"));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .fileResolver(tempDir)
                .build();

        // Collect byte pipeline output
        var output = new ByteArrayOutputStream();
        mapper.mapToNTriplesBytes().toStream().forEach(bytes -> {
            try {
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Parse once
        var model1 = parseNTriples(output.toByteArray());

        // Serialize model1 back to NT via FastNTriplesSerializer, then parse again
        var reserializedOutput = new ByteArrayOutputStream();
        FastNTriplesSerializer.withDefaults().serialize(Flux.fromIterable(model1), reserializedOutput);
        var model2 = parseNTriples(reserializedOutput.toByteArray());

        assertThat("Round-tripped model must be isomorphic to original", isomorphic(model1, model2), is(true));
    }

    private static Model parseNTriples(byte[] data) throws IOException {
        return Rio.parse(new ByteArrayInputStream(data), RDFFormat.NTRIPLES);
    }

    private static Model parseNQuads(byte[] data) throws IOException {
        return Rio.parse(new ByteArrayInputStream(data), RDFFormat.NQUADS);
    }
}
