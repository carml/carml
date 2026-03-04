package io.carml.model;

import io.carml.model.Template.ReferenceExpression;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Value;

public interface ExpressionMap extends Resource {

    Value getConstant();

    String getReference();

    Template getTemplate();

    TriplesMap getFunctionValue();

    /**
     * Returns the {@link FunctionExecution} associated with this expression map, if any. This is the
     * RML-FNML path for function-based term generation, as opposed to the legacy
     * {@link #getFunctionValue()} path.
     *
     * @return the function execution, or {@code null} if not set
     */
    default FunctionExecution getFunctionExecution() {
        return null;
    }

    /**
     * Returns the {@link ReturnMap} selecting which return value to use from the
     * {@link #getFunctionExecution()}, if any.
     *
     * @return the return map, or {@code null} if not set
     */
    default ReturnMap getReturnMap() {
        return null;
    }

    /**
     * Returns the set of {@link Condition}s that gate this expression map. If any condition
     * evaluates to false, the expression map produces no value.
     *
     * @return the conditions, or an empty set if none are set
     */
    default Set<Condition> getConditions() {
        return Set.of();
    }

    default Set<String> getExpressionMapExpressionSet() {
        Stream<String> ownExpressions;

        if (getReference() != null) {
            ownExpressions = Stream.of(getReference());
        } else if (getTemplate() != null) {
            ownExpressions = getTemplate().getReferenceExpressions().stream().map(ReferenceExpression::getValue);
        } else if (getFunctionValue() != null) {
            ownExpressions = getFunctionValue().getPredicateObjectMaps().stream()
                    .flatMap(predicateObjectMap -> Stream.concat(
                            predicateObjectMap.getPredicateMaps().stream()
                                    .flatMap(predicateMap -> predicateMap.getExpressionMapExpressionSet().stream()),
                            predicateObjectMap.getObjectMaps().stream()
                                    .filter(ObjectMap.class::isInstance)
                                    .map(ObjectMap.class::cast)
                                    .flatMap(objectMap -> objectMap.getExpressionMapExpressionSet().stream())));
        } else if (getFunctionExecution() != null) {
            ownExpressions = getFunctionExecution().getInputs().stream()
                    .map(Input::getInputValueMap)
                    .flatMap(inputValueMap -> inputValueMap.getExpressionMapExpressionSet().stream());
        } else {
            ownExpressions = Stream.empty();
        }

        var conditionExpressions = getConditions().stream().flatMap(ExpressionMap::collectConditionExpressions);

        return Stream.concat(ownExpressions, conditionExpressions).collect(Collectors.toUnmodifiableSet());
    }

    private static Stream<String> collectConditionExpressions(Condition condition) {
        var shortcutRefs = Stream.of(
                        condition.getIsNull() != null ? Stream.of(condition.getIsNull()) : Stream.<String>empty(),
                        condition.getIsNotNull() != null ? Stream.of(condition.getIsNotNull()) : Stream.<String>empty(),
                        condition.getEquals().stream(),
                        condition.getNotEquals().stream())
                .flatMap(s -> s);

        var fnExecRefs = condition.getFunctionExecution() != null
                ? condition.getFunctionExecution().getInputs().stream()
                        .map(Input::getInputValueMap)
                        .flatMap(inputValueMap -> inputValueMap.getExpressionMapExpressionSet().stream())
                : Stream.<String>empty();

        return Stream.concat(shortcutRefs, fnExecRefs);
    }
}
