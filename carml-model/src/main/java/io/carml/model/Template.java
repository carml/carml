package io.carml.model;

import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

public interface Template {

    List<Segment> getSegments();

    Set<ReferenceExpression> getReferenceExpressions();

    Map<ExpressionSegment, ReferenceExpression> getExpressionSegmentMap();

    String toTemplateString();

    Template adaptExpressions(UnaryOperator<String> referenceExpressionAdapter);

    interface Segment {

        String getValue();

        String getTemplateStringRepresentation();
    }

    interface ReferenceExpression {

        String getValue();
    }
}
