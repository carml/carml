package io.carml.model.impl.template;

import static java.util.Map.entry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import io.carml.model.Template.ReferenceExpression;
import io.carml.model.impl.CarmlTemplate;
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
                        List.of("foo1-bar1", "foo1-bar2")),
                // Duplicate expression values: duplicates should be preserved in output
                Arguments.of(
                        "{foo}",
                        List.of(entry("foo", (Function<ReferenceExpression, List<String>>) expr -> List.of("a", "a"))),
                        List.of("a", "a")),
                // Duplicate expression values in multi-reference template
                Arguments.of(
                        "{foo}-{bar}",
                        List.of(
                                entry("foo", (Function<ReferenceExpression, List<String>>) expr -> List.of("x", "x")),
                                entry("bar", (Function<ReferenceExpression, List<String>>) expr -> List.of("1"))),
                        List.of("x-1", "x-1")));
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
        if (expected.isEmpty()) {
            assertThat(evaluationResult, is(empty()));
        } else {
            assertThat(evaluationResult, contains(expected.toArray()));
        }
    }
}
