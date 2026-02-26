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
        if (getReference() != null) {
            return Set.of(getReference());
        }

        if (getTemplate() != null) {
            return getTemplate().getReferenceExpressions().stream()
                    .map(ReferenceExpression::getValue)
                    .collect(Collectors.toUnmodifiableSet());
        }

        if (getFunctionValue() != null) {
            return getFunctionValue().getPredicateObjectMaps().stream()
                    .flatMap(predicateObjectMap -> Stream.concat(
                            predicateObjectMap.getPredicateMaps().stream()
                                    .flatMap(predicateMap -> predicateMap.getExpressionMapExpressionSet().stream()),
                            predicateObjectMap.getObjectMaps().stream()
                                    .filter(ObjectMap.class::isInstance)
                                    .map(ObjectMap.class::cast)
                                    .flatMap(objectMap -> objectMap.getExpressionMapExpressionSet().stream())))
                    .collect(Collectors.toUnmodifiableSet());
        }

        if (getFunctionExecution() != null) {
            return getFunctionExecution().getInputs().stream()
                    .map(Input::getInputValueMap)
                    .flatMap(inputValueMap -> inputValueMap.getExpressionMapExpressionSet().stream())
                    .collect(Collectors.toUnmodifiableSet());
        }

        return Set.of();
    }
}
