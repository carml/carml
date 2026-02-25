package io.carml.logicalview;

import io.carml.model.Field;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlExpressionField;
import io.carml.model.impl.CarmlLogicalView;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

class ImplicitViewFactory {

    private ImplicitViewFactory() {}

    static LogicalView wrap(TriplesMap triplesMap) {
        Objects.requireNonNull(triplesMap, "triplesMap must not be null");
        var logicalSource = triplesMap.getLogicalSource();
        Objects.requireNonNull(logicalSource, "triplesMap must have a logicalSource");

        var referenceExpressions = triplesMap.getReferenceExpressionSet();

        Set<Field> fields = referenceExpressions.stream()
                .map(expression -> (Field) CarmlExpressionField.builder()
                        .fieldName(expression)
                        .reference(expression)
                        .build())
                .collect(Collectors.toUnmodifiableSet());

        return CarmlLogicalView.builder().viewOn(logicalSource).fields(fields).build();
    }
}
