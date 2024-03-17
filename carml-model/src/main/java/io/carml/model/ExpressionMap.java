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

        return Set.of();
    }
}
