package io.carml.engine;

import static java.util.Map.entry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import io.carml.model.Template.ReferenceExpression;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.template.TemplateParser;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TemplateEvaluationTest {

    static Stream<Arguments> templateSource() {
        return Stream.of(
                Arguments.of("foo-bar", List.of(), List.of("foo-bar")),
                Arguments.of(
                        "{foo}-{bar}",
                        List.of(
                                entry("foo", (Function<ReferenceExpression, List<String>>) expr -> List.of()),
                                entry("bar", (Function<ReferenceExpression, List<String>>)
                                        expr -> List.of("bar1", "bar2"))),
                        List.of()),
                Arguments.of(
                        "{foo}-{bar}",
                        List.of(
                                entry("foo", (Function<ReferenceExpression, List<String>>) expr -> List.of("foo1")),
                                entry("bar", (Function<ReferenceExpression, List<String>>)
                                        expr -> List.of("bar1", "bar2"))),
                        List.of("foo1-bar1", "foo1-bar2")));
    }

    @ParameterizedTest
    @MethodSource("templateSource")
    void givenTemplateAndBindings_whenGetTemplateEvaluation_thenReturnExpectedResults(
            String template,
            List<Entry<String, Function<ReferenceExpression, List<String>>>> bindings,
            List<String> expected) {
        // Given
        var templateEvaluationBuilder = TemplateEvaluation.builder()
                .template(TemplateParser.getInstance().parse(template));

        for (int i = 0; i < bindings.size(); i++) {
            var entry = bindings.get(i);
            var expr = CarmlTemplate.CarmlReferenceExpression.of(i, entry.getKey());
            templateEvaluationBuilder.bind(expr, entry.getValue());
        }

        var templateEvaluation = templateEvaluationBuilder.build();

        // When
        var evaluationResult = templateEvaluation.get();

        // Then
        assertThat(evaluationResult, containsInAnyOrder(expected.toArray()));
    }
}
