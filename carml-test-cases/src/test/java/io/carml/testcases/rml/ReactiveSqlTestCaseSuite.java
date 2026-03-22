package io.carml.testcases.rml;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.sql.ReactiveSqlLogicalViewEvaluatorFactory;
import io.carml.logicalview.sql.SqlClientProvider;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.util.RmlMappingLoader;
import io.vertx.core.Vertx;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for conformance test suites that run against the reactive SQL evaluator. Subclasses
 * provide a {@link SqlClientProvider}, the actual JDBC URL, and optionally a password override for
 * DSN/credential substitution in mapping files.
 *
 * <p>W3C conformance test mapping files use {@code "CONNECTIONDSN"} as a placeholder JDBC DSN and
 * typically have {@code d2rq:password ""} (empty). This class substitutes the DSN and password
 * before parsing so the mapping connects to the actual Testcontainers database.
 */
public abstract class ReactiveSqlTestCaseSuite extends RmlTestCaseSuite {

    private Vertx vertx;

    private ReactiveSqlLogicalViewEvaluatorFactory evaluatorFactory;

    protected abstract List<SqlClientProvider> getProviders();

    protected abstract String getJdbcUrl();

    /**
     * Returns the password to substitute in mapping files. The W3C test mappings use
     * {@code d2rq:password ""} (empty), but Testcontainers databases require a non-empty password.
     * This substitutes {@code d2rq:password ""} with {@code d2rq:password "<password>"}.
     */
    protected abstract String getPassword();

    @BeforeAll
    void setUpReactiveSql() {
        vertx = Vertx.vertx();
        evaluatorFactory = new ReactiveSqlLogicalViewEvaluatorFactory(getProviders(), vertx);
    }

    @AfterAll
    void tearDownReactiveSql() {
        if (evaluatorFactory != null) {
            evaluatorFactory.close();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(evaluatorFactory);

        Optional.ofNullable(testCase.getDefaultBaseIri()).or(this::getBaseIri).ifPresent(mapperBuilder::baseIri);

        var mappingStream =
                getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument());
        Set<TriplesMap> mapping = loadMappingWithSubstitutions(mappingStream);

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private Set<TriplesMap> loadMappingWithSubstitutions(java.io.InputStream mappingStream) {
        String mappingText;
        try {
            mappingText = new String(mappingStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read mapping file", e);
        }

        var substituted = mappingText
                .replace("CONNECTIONDSN", getJdbcUrl())
                .replace("d2rq:password \"\"", "d2rq:password \"%s\"".formatted(getPassword()));
        var substitutedStream = new ByteArrayInputStream(substituted.getBytes(StandardCharsets.UTF_8));
        return RmlMappingLoader.build().load(RDFFormat.TURTLE, substitutedStream);
    }
}
