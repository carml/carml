package io.carml.testcases.util;

import static io.carml.util.ModelSerializer.SIMPLE_WRITER_CONFIG;
import static java.util.function.Predicate.not;
import static org.eclipse.rdf4j.model.util.Statements.statement;

import io.carml.testcases.model.Database;
import io.carml.testcases.model.Input;
import io.carml.testcases.model.TripleStore;
import io.carml.util.Models;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;

public class TestCaseProcessor {

    private static final Set<String> SQL_TYPE = Set.of("MySQL", "PostgreSQL");

    public static void main(String[] args) {
        try {
            fixSqlTestCases("rml/io/test-cases");
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void fixSqlTestCases(String resource) throws URISyntaxException, IOException {
        var path = Paths.get(ClassLoader.getSystemResource(resource).toURI());

        try (var testCasesDir = Files.walk(path, 1)) {
            testCasesDir
                    .filter(Files::isDirectory)
                    .filter(not(path::equals))
                    // .filter(testCasePath -> testCasePath.getFileName().toString().endsWith("SPARQL"))
                    .forEach(TestCaseProcessor::fixTestCase);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void fixTestCase(Path testcasePath) {
        var files = getFiles(testcasePath);
        var mapping = files.stream()
                .filter(path -> path.getFileName().toString().equals("mapping.ttl"))
                .findFirst()
                .orElseThrow();

        try {
            var turtle = new String(Files.readAllBytes(mapping));

            var prefixes = parsePrefixes(turtle);

            Model mappingModel;
            try {
                mappingModel = Models.parse(Files.newInputStream(mapping), RDFFormat.TURTLE);
            } catch (RDFParseException e) {
                System.out.println(String.format("Error parsing mapping file: %s%n%s", mapping, e));
                return;
            }

            // fix
            var outModel = fixMapping(mappingModel, prefixes);

            Rio.write(
                    outModel,
                    Files.newOutputStream(mapping),
                    RDFFormat.TURTLE,
                    SIMPLE_WRITER_CONFIG.apply(new WriterConfig()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Model fixMapping(Model mappingModel, List<Namespace> prefixes) {
        var fixed =
                mappingModel.stream().flatMap(TestCaseProcessor::statementFixes).collect(ModelCollector.toModel());

        prefixes.forEach(fixed::setNamespace);

        return fixed;
    }

    private static Stream<Statement> statementFixes(Statement original) {
        // if (original.getObject()
        // .stringValue()
        // .equals(Rml.NAMESPACE + "SQL2008TableName")) {
        // return Stream
        // .of(statement(original.getSubject(), original.getPredicate(), Rdf.Rml.SQL2008Table,
        // original.getContext()));
        // }
        //
        // if (original.getPredicate()
        // .stringValue()
        // .equals(Rml.NAMESPACE + "sqlVersion")) {
        // return Stream.of();
        // }
        //
        // if (original.getPredicate()
        // .stringValue()
        // .equals(Rml.NAMESPACE + "tableName")) {
        // return Stream.of(statement(original.getSubject(), Rdf.Rml.iterator, original.getObject(),
        // original.getContext()));
        // }

        if (original.getPredicate().stringValue().equals(Rml.NAMESPACE + "path")) {
            return Stream.of(
                    statement(original.getSubject(), Rdf.Rml.root, Rdf.Rml.MappingDirectory, original.getContext()),
                    original);
        }

        // if (original.getPredicate().stringValue().equals(Rml.NAMESPACE + "iterator") &&
        // original.getObject().stringValue().equals("$.results.bindings[*]")) {
        // return Stream.of();
        // }
        //
        // if (original.getPredicate().stringValue().equals(Rml.NAMESPACE + "referenceFormulation")) {
        // return Stream.of(statement(original.getSubject(), original.getPredicate(),
        // iri("https://www.w3.org/ns/formats/SPARQL_Results_JSON"), original.getContext()));
        // }
        //
        // if (original.getPredicate().stringValue().equals(SD.NAMESPACE + "resultFormat")){
        // return Stream.of();
        // }

        return Stream.of(original);
    }

    private static List<Namespace> parsePrefixes(String turtle) throws IOException {
        return turtle.lines()
                .filter(line -> line.startsWith("@prefix"))
                .map(line -> line.substring(8, line.length() - 1))
                .map(prefix -> prefix.split(" "))
                .map(parts -> (Namespace) new SimpleNamespace(
                        parts[0].replaceAll(":$", "").trim(),
                        parts[1].replace("<", "")
                                .replace(">", "")
                                .replaceAll("\\.$", "")
                                .trim()))
                .toList();
    }

    private static List<Path> getFiles(Path testcasePath) {
        try (var testCaseFiles = Files.walk(testcasePath)) {
            return testCaseFiles.filter(Files::isRegularFile).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void processSqlTestCase(
            Input.InputBuilder<?, ?> inputBuilder, String testCaseName, List<Path> files) {
        var databaseBuilder = Database.builder().id(String.format("database-%s", testCaseName));

        files.stream()
                .map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.endsWith(".sql"))
                .forEach(databaseBuilder::sqlScriptFile);

        inputBuilder.database(databaseBuilder.build());
    }

    private static void processSparqlTestCase(
            Input.InputBuilder<?, ?> inputBuilder, String testCaseName, List<Path> files) {
        var tripleStoreBuilder = TripleStore.builder().id(String.format("triplestore-%s", testCaseName));

        files.stream()
                .map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.endsWith(".ttl"))
                .filter(not(fileName -> fileName.equals("mapping.ttl")))
                .forEach(tripleStoreBuilder::rdfFile);

        inputBuilder.tripleStore(tripleStoreBuilder.build());
    }

    private static void processOtherTestCase(Input.InputBuilder<?, ?> inputBuilder, List<Path> files) {
        files.stream()
                .map(path -> path.getFileName().toString())
                .filter(not(fileName -> fileName.endsWith(".sql")))
                .filter(not(fileName -> fileName.endsWith(".ttl")))
                .filter(not(fileName -> fileName.equals("output.nq")))
                .forEach(inputBuilder::inputFile);
    }
}
