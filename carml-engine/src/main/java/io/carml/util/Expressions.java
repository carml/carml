package io.carml.util;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.template.Template;
import io.carml.engine.template.TemplateParser;
import io.carml.model.ExpressionMap;
import io.carml.model.GraphMap;
import io.carml.model.Join;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlGraphMap;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlPredicateMap;
import io.carml.model.impl.CarmlPredicateObjectMap;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.model.impl.CarmlTriplesMap;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Expressions {


  public static Set<String> getExpressions(TriplesMap triplesMap) {
    return getExpressions(triplesMap, triplesMap.getPredicateObjectMaps());
  }

  public static Set<String> getExpressions(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMapFilter) {
    return getTriplesMapExpressions(triplesMap, predicateObjectMapFilter).collect(toUnmodifiableSet());
  }

  public static Set<String> getExpressions(SubjectMap subjectMap) {
    return getSubjectMapExpressions(subjectMap).collect(toUnmodifiableSet());
  }

  public static Set<String> getExpressions(PredicateObjectMap predicateObjectMap) {
    return getPredicateObjectMapExpressions(predicateObjectMap).collect(toUnmodifiableSet());
  }


  private static Stream<String> getTriplesMapExpressions(TriplesMap triplesMap,
      Set<PredicateObjectMap> predicateObjectMapFilter) {
    var subjectMapExpressions = triplesMap.getSubjectMaps()
        .stream()
        .flatMap(Expressions::getSubjectMapExpressions);

    var pomExpressions = predicateObjectMapFilter.stream()
        .flatMap(Expressions::getPredicateObjectMapExpressions);

    return Stream.concat(subjectMapExpressions, pomExpressions);
  }

  private static Stream<String> getSubjectMapExpressions(SubjectMap subjectMap) {
    var subjectExpressions = getExpressionMapExpressions(subjectMap);
    var graphMapExpressions = subjectMap.getGraphMaps()
        .stream()
        .flatMap(Expressions::getExpressionMapExpressions);

    return Stream.concat(subjectExpressions, graphMapExpressions);
  }

  private static Stream<String> getExpressionMapExpressions(ExpressionMap expressionMap) {
    if (expressionMap.getConstant() != null) {
      return Stream.of();
    } else if (expressionMap.getReference() != null) {
      return Stream.of(expressionMap.getReference());
    } else if (expressionMap.getTemplate() != null) {
      return TemplateParser.build()
          .parse(expressionMap.getTemplate())
          .getExpressions()
          .stream()
          .map(Template.Expression::getValue);
    } else if (expressionMap.getFunctionValue() != null) {
      var functionValue = expressionMap.getFunctionValue();
      return getTriplesMapExpressions(functionValue, functionValue.getPredicateObjectMaps());
    }

    return Stream.of();
  }

  private static Stream<String> getPredicateObjectMapExpressions(PredicateObjectMap predicateObjectMap) {
    var predicateMapExpressions = predicateObjectMap.getPredicateMaps()
        .stream()
        .flatMap(Expressions::getExpressionMapExpressions);

    var graphMapExpressions = predicateObjectMap.getGraphMaps()
        .stream()
        .flatMap(Expressions::getExpressionMapExpressions);

    var objectMapExpressions = predicateObjectMap.getObjectMaps()
        .stream()
        .filter(ObjectMap.class::isInstance)
        .map(ObjectMap.class::cast)
        .flatMap(Expressions::getExpressionMapExpressions);

    var refObjectMapExpressions = predicateObjectMap.getObjectMaps()
        .stream()
        .filter(RefObjectMap.class::isInstance)
        .map(RefObjectMap.class::cast)
        .map(RefObjectMap::getJoinConditions)
        .flatMap(Set::stream)
        .map(Join::getChild);

    return Stream.concat(predicateMapExpressions,
        Stream.concat(graphMapExpressions, Stream.concat(objectMapExpressions, refObjectMapExpressions)));
  }

  public static SubjectMap applyExpressionPrefix(String expressionPrefix, @NonNull SubjectMap subjectMap) {
    if (expressionPrefix == null) {
      return subjectMap;
    }

    var builder = toCarmlSubjectMap(subjectMap).toBuilder();
    if (subjectMap.getReference() != null) {
      processReference(expressionPrefix, subjectMap, builder::reference);
      return builder.build();
    } else if (subjectMap.getTemplate() != null) {
      processTemplate(expressionPrefix, subjectMap, builder::template);
      return builder.build();
    } else if (subjectMap.getFunctionValue() != null) {
      processFunctionValue(expressionPrefix, subjectMap, builder::functionValue);
      return builder.build();
    } else {
      return subjectMap;
    }
  }

  public static PredicateMap applyExpressionPrefix(String expressionPrefix, @NonNull PredicateMap predicateMap) {
    if (expressionPrefix == null) {
      return predicateMap;
    }

    var builder = toCarmlPredicateMap(predicateMap).toBuilder();
    if (predicateMap.getReference() != null) {
      processReference(expressionPrefix, predicateMap, builder::reference);
      return builder.build();
    } else if (predicateMap.getTemplate() != null) {
      processTemplate(expressionPrefix, predicateMap, builder::template);
      return builder.build();
    } else if (predicateMap.getFunctionValue() != null) {
      processFunctionValue(expressionPrefix, predicateMap, builder::functionValue);
      return builder.build();
    } else {
      return predicateMap;
    }
  }

  public static ObjectMap applyExpressionPrefix(String expressionPrefix, @NonNull ObjectMap objectMap) {
    if (expressionPrefix == null) {
      return objectMap;
    }

    var builder = toCarmlObjectMap(objectMap).toBuilder();
    if (objectMap.getReference() != null) {
      processReference(expressionPrefix, objectMap, builder::reference);
      return builder.build();
    } else if (objectMap.getTemplate() != null) {
      processTemplate(expressionPrefix, objectMap, builder::template);
      return builder.build();
    } else if (objectMap.getFunctionValue() != null) {
      processFunctionValue(expressionPrefix, objectMap, builder::functionValue);
      return builder.build();
    } else {
      return objectMap;
    }
  }

  public static GraphMap applyExpressionPrefix(String expressionPrefix, @NonNull GraphMap graphMap) {
    if (expressionPrefix == null) {
      return graphMap;
    }

    var builder = toCarmlGraphMap(graphMap).toBuilder();
    if (graphMap.getReference() != null) {
      processReference(expressionPrefix, graphMap, builder::reference);
      return builder.build();
    } else if (graphMap.getTemplate() != null) {
      processTemplate(expressionPrefix, graphMap, builder::template);
      return builder.build();
    } else if (graphMap.getFunctionValue() != null) {
      processFunctionValue(expressionPrefix, graphMap, builder::functionValue);
      return builder.build();
    } else {
      return graphMap;
    }
  }

  private static void processReference(String expressionPrefix, ExpressionMap expressionMap,
      Consumer<String> referenceApplier) {
    referenceApplier.accept(String.format("%s%s", expressionPrefix, expressionMap.getReference()));
  }

  private static void processTemplate(String expressionPrefix, ExpressionMap expressionMap,
      Consumer<String> templateApplier) {
    var templateParser = TemplateParser.buildWithExpressionPrefix(expressionPrefix);
    var prefixedTemplate = templateParser.parse(expressionMap.getTemplate())
        .toTemplateString();
    templateApplier.accept(prefixedTemplate);
  }

  private static void processFunctionValue(String expressionPrefix, ExpressionMap expressionMap,
      Consumer<TriplesMap> functionValueApplier) {
    var fnBuilder = CarmlTriplesMap.builder();
    var functionValue = expressionMap.getFunctionValue();

    functionValue.getSubjectMaps()
        .stream()
        .map(subjectMap -> applyExpressionPrefix(expressionPrefix, subjectMap))
        .forEach(fnBuilder::subjectMap);

    functionValue.getPredicateObjectMaps()
        .stream()
        .map(pom -> processPredicateObjectMap(expressionPrefix, pom))
        .forEach(fnBuilder::predicateObjectMap);

    functionValueApplier.accept(fnBuilder.build());
  }

  private static PredicateObjectMap processPredicateObjectMap(String expressionPrefix,
      PredicateObjectMap predicateObjectMap) {
    var pomBuilder = CarmlPredicateObjectMap.builder();

    predicateObjectMap.getPredicateMaps()
        .stream()
        .map(predicateMap -> applyExpressionPrefix(expressionPrefix, predicateMap))
        .forEach(pomBuilder::predicateMap);

    // TODO refObjectMap in functionValue?
    predicateObjectMap.getObjectMaps()
        .stream()
        .filter(ObjectMap.class::isInstance)
        .map(ObjectMap.class::cast)
        .map(objectMap -> applyExpressionPrefix(expressionPrefix, objectMap))
        .forEach(pomBuilder::objectMap);

    return pomBuilder.build();
  }

  private static CarmlSubjectMap toCarmlSubjectMap(SubjectMap subjectMap) {
    if (!(subjectMap instanceof CarmlSubjectMap)) {
      throw new UnsupportedOperationException("Currently only CarmlSubjectMap supported.");
    }
    return (CarmlSubjectMap) subjectMap;
  }

  private static CarmlPredicateMap toCarmlPredicateMap(PredicateMap predicateMap) {
    if (!(predicateMap instanceof CarmlPredicateMap)) {
      throw new UnsupportedOperationException("Currently only CarmlPredicateMap supported.");
    }
    return (CarmlPredicateMap) predicateMap;
  }

  private static CarmlObjectMap toCarmlObjectMap(ObjectMap objectMap) {
    if (!(objectMap instanceof CarmlObjectMap)) {
      throw new UnsupportedOperationException("Currently only CarmlObjectMap supported.");
    }
    return (CarmlObjectMap) objectMap;
  }

  private static CarmlGraphMap toCarmlGraphMap(GraphMap graphMap) {
    if (!(graphMap instanceof CarmlGraphMap)) {
      throw new UnsupportedOperationException("Currently only CarmlGraphMap supported.");
    }
    return (CarmlGraphMap) graphMap;
  }
}
