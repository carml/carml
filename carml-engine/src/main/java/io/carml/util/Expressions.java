package io.carml.util;

import io.carml.model.ExpressionMap;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Expressions {

    private static void processReference(
            String expressionPrefix, ExpressionMap expressionMap, Consumer<String> referenceApplier) {
        referenceApplier.accept(String.format("%s%s", expressionPrefix, expressionMap.getReference()));
    }
}
