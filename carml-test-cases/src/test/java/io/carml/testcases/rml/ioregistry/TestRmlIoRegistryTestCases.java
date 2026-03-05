package io.carml.testcases.rml.ioregistry;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;

class TestRmlIoRegistryTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // MySQL resolver module removed (replaced by DuckDB mysql_scanner)
                "RMLIOREGTC0004",

                // PostgreSQL resolver module removed (replaced by DuckDB postgres_scanner)
                "RMLIOREGTC0005",

                // SQL Server resolver module removed (replaced by DuckDB mssql extension)
                "RMLIOREGTC0006",

                // Test case bug: hasError=true but the spec says non-matching references produce NULL, not
                // an error. The JSONPath IO-Registry spec (section "Generation of null values") states that
                // selectors referring to non-existent JSON names result in NULL. The XPath IO-Registry spec
                // (section "Handling absence of values") states that non-existent XPath evaluates to NULL.
                // RML-Core term generation rule 1: "If the value is null, empty or missing, then no RDF term
                // is generated." The output algorithm then skips the triple — no error is raised.
                "RMLIOREGTC0002b", // JSON: $.THIS_VALUE_DOES_NOT_EXIST → NULL, not error
                "RMLIOREGTC0003b", // XML: NON_EXISTING → NULL, not error

                // Test case bug: namespace URL mismatch ("http://example.org" vs XML's "http://example.org/")
                // and unprefixed names in iterator/references for elements in the default namespace.
                // CARML's namespace pipeline works correctly (verified with corrected test case data).
                "RMLIOREGTC0003d",

                // Test case bug: expected output has plain "33" for JSON integer; engine produces "33"^^xsd:integer
                "RMLIOREGTC0007a",

                // Unsupported source type: WoT (td:Thing) source descriptions
                "RMLIOREGTC0008a", // HTTP JSON API
                "RMLIOREGTC0009a", // Kafka stream
                "RMLIOREGTC0010a", // MQTT stream

                // Unsupported source type: SPARQL endpoint (sd:Service)
                "RMLIOREGTC0011a",

                // Test case bug: CSVW quoteChar test case bug: mapping references uppercase {ID}/{Name} but CSV headers
                // are lowercase id/name/age
                "RMLIOREGTC0012i");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        byte[] mappingBytes;
        try {
            mappingBytes = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument())
                    .readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, new ByteArrayInputStream(mappingBytes));

        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }
}
