package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Models.isomorphic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.model.Mapping;
import io.carml.util.Models;
import java.util.Objects;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

/**
 * Integration test covering function-valued {@code rml:childMap} and {@code rml:parentMap} in
 * join conditions. The mappings use {@code rml:functionExecution} to apply string-case functions
 * to one side of the join key so that the child and parent literals (lower-case in one file,
 * upper-case in the other) compare equal.
 *
 * <p>The mapper is built with a shared {@link io.carml.functions.FunctionRegistry} — the default
 * {@link io.carml.logicalview.DefaultLogicalViewEvaluatorFactory} picked up by the builder wires
 * a {@link io.carml.logicalview.DefaultExpressionMapEvaluator} backed by that registry, so
 * functions declared via {@link RdfRmlMapper.Builder#function(String)} resolve uniformly in term
 * generation and join-key evaluation.
 */
class RdfRmlMapperFunctionJoinIntegrationTest {

    private static final String UPPER_CASE_IRI = "http://example.com/ns#toUpperCase";

    private static final String LOWER_CASE_IRI = "http://example.com/ns#toLowerCase";

    private static final String INPUT_PARAM_IRI = "http://example.com/ns#inputParam";

    @Test
    void mapWithFunctionChildMap_producesExpectedJoinedTriples() {
        var mapping = loadMapping();

        var mapper = RdfRmlMapper.builder()
                .classPathResolver("RdfRmlMapperFunctionJoin")
                .mapping(mapping)
                .function(UPPER_CASE_IRI)
                .param(INPUT_PARAM_IRI, String.class)
                .returns(String.class)
                .execute(params -> {
                    var value = params.get(SimpleValueFactory.getInstance().createIRI(INPUT_PARAM_IRI));
                    return value == null ? null : value.toString().toUpperCase();
                })
                .build();

        var actual = mapper.map().collect(ModelCollector.toTreeModel()).block();
        var expected = loadExpected();

        assertThat(isomorphic(actual, expected), is(true));
    }

    @Test
    void mapWithFunctionParentMap_producesExpectedJoinedTriples() {
        // Mirror of the child-side case, but with the function applied on the PARENT side instead.
        // persons.json has lower-case city strings; cities.json has upper-case name strings. The
        // parent-side function downcases `name` so the join key matches the plain child reference.
        // Exercises the parent-row evaluation path in DefaultLogicalViewEvaluator — distinct hot
        // path from the child-row path covered in the first test.
        var mapping = Mapping.of(
                RDFFormat.TURTLE,
                Objects.requireNonNull(RdfRmlMapperFunctionJoinIntegrationTest.class.getResourceAsStream(
                        "/RdfRmlMapperFunctionJoin/mapping-parent-side.rml.ttl")));

        var mapper = RdfRmlMapper.builder()
                .classPathResolver("RdfRmlMapperFunctionJoin")
                .mapping(mapping)
                .function(LOWER_CASE_IRI)
                .param(INPUT_PARAM_IRI, String.class)
                .returns(String.class)
                .execute(params -> {
                    var value = params.get(SimpleValueFactory.getInstance().createIRI(INPUT_PARAM_IRI));
                    return value == null ? null : value.toString().toLowerCase();
                })
                .build();

        var actual = mapper.map().collect(ModelCollector.toTreeModel()).block();
        var expected = loadExpected();

        assertThat(isomorphic(actual, expected), is(true));
    }

    private static Mapping loadMapping() {
        return Mapping.of(
                RDFFormat.TURTLE,
                Objects.requireNonNull(RdfRmlMapperFunctionJoinIntegrationTest.class.getResourceAsStream(
                        "/RdfRmlMapperFunctionJoin/mapping.rml.ttl")));
    }

    private static Model loadExpected() {
        return Models.parse(
                        Objects.requireNonNull(RdfRmlMapperFunctionJoinIntegrationTest.class.getResourceAsStream(
                                "/RdfRmlMapperFunctionJoin/expected.ttl")),
                        RDFFormat.TURTLE)
                .stream()
                .collect(ModelCollector.toTreeModel());
    }
}
