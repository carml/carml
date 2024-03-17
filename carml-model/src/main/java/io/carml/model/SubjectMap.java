package io.carml.model;

import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;

public interface SubjectMap extends TermMap {

    Set<IRI> getClasses();

    Set<GraphMap> getGraphMaps();

    SubjectMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter);

    default Set<String> getReferenceExpressionSet() {
        var expressionMapExpressions = getExpressionMapExpressionSet().stream();
        var graphMapExpressions = getGraphMaps().stream()
                .map(GraphMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        return Stream.concat(expressionMapExpressions, graphMapExpressions).collect(Collectors.toUnmodifiableSet());
    }
}
