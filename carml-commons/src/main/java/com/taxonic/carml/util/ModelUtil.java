package com.taxonic.carml.util;

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class ModelUtil {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  private ModelUtil() {}

  public static Model describeResource(Model model, Resource resource) {
    return model.filter(resource, null, null)
        .stream()
        .flatMap(statement -> {
          Value object = statement.getObject();
          return object instanceof BNode
              ? Stream.concat(Stream.of(statement), describeResource(model, (BNode) object).stream())
              : Stream.of(statement);
        })
        .collect(Collectors.toCollection(LinkedHashModel::new));
  }

  public static Model reverseDescribeResource(Model model, Resource resource) {
    return model.filter(null, null, resource)
        .stream()
        .flatMap(statement -> {
          Resource subject = statement.getSubject();
          return subject instanceof BNode
              ? Stream.concat(Stream.of(statement), reverseDescribeResource(model, subject).stream())
              : Stream.of(statement);
        })
        .collect(Collectors.toCollection(LinkedHashModel::new));
  }

  public static Model symmetricDescribeResource(Model model, Resource resource) {
    Model description = reverseDescribeResource(model, resource);
    description.addAll(describeResource(model, resource));
    return description;
  }

  public static Model getCartesianProductStatements(Set<Resource> subjects, Set<IRI> predicates,
      Set<? extends Value> objects, Set<Resource> graphs) {
    return streamCartesianProductStatements(subjects, predicates, objects, graphs, graph -> graph, VALUE_FACTORY)
        .collect(Collectors.toCollection(LinkedHashModel::new));
  }

  public static Model getCartesianProductStatements(Set<Resource> subjects, Set<IRI> predicates,
      Set<? extends Value> objects, Set<Resource> graphs, ValueFactory valueFactory) {
    return streamCartesianProductStatements(subjects, predicates, objects, graphs, graph -> graph, valueFactory)
        .collect(Collectors.toCollection(LinkedHashModel::new));
  }

  public static Stream<Statement> streamCartesianProductStatements(Set<Resource> subjects, Set<IRI> predicates,
      Set<? extends Value> objects, Set<Resource> graphs) {
    return streamCartesianProductStatements(subjects, predicates, objects, graphs, graph -> graph, VALUE_FACTORY);
  }

  @SafeVarargs
  public static Stream<Statement> streamCartesianProductStatements(Set<Resource> subjects, Set<IRI> predicates,
      Set<? extends Value> objects, Set<Resource> graphs, UnaryOperator<Resource> graphModifier,
      ValueFactory valueFactory, Consumer<Statement>... statementConsumers) {
    if (subjects.isEmpty() || predicates.isEmpty() || objects.isEmpty()) {
      throw new ModelUtilException(
          "Could not create cartesian product statements because at least one of subjects, predicates or objects was"
              + " empty");
    }
    if (graphs.isEmpty()) {
      return Sets.cartesianProduct(subjects, predicates, objects)
          .stream()
          .map(element -> createStatement(element.get(0), element.get(1), element.get(2), null, graphModifier,
              valueFactory, statementConsumers));
    } else {
      return Sets.cartesianProduct(subjects, predicates, objects, graphs)
          .stream()
          .map(element -> createStatement(element.get(0), element.get(1), element.get(2), element.get(3), graphModifier,
              valueFactory, statementConsumers));
    }
  }

  public static Statement createStatement(Value subjectValue, Value predicateValue, Value object, Value contextValue) {
    return createStatement(subjectValue, predicateValue, object, contextValue, context -> context, VALUE_FACTORY);
  }

  @SafeVarargs
  public static Statement createStatement(Value subjectValue, Value predicateValue, Value object, Value contextValue,
      UnaryOperator<Resource> contextModifier, ValueFactory valueFactory, Consumer<Statement>... statementConsumers) {
    Resource subject;
    if (subjectValue instanceof Resource) {
      subject = (Resource) subjectValue;
    } else {
      throw new ModelUtilException(String.format("Expected subjectValue `%s` to be instance of Resource, but was %s",
          subjectValue, subjectValue.getClass()));
    }

    IRI predicate;
    if (predicateValue instanceof Resource) {
      predicate = (IRI) predicateValue;
    } else {
      throw new ModelUtilException(String.format("Expected predicateValue `%s` to be instance of IRI, but was %s",
          predicateValue, predicateValue.getClass()));
    }

    Resource context;
    if (contextValue != null) {
      if (contextValue instanceof Resource) {
        context = contextModifier.apply((Resource) contextValue);
      } else {
        throw new ModelUtilException(String.format("Expected contextValue `%s` to be instance of Resource, but was %s",
            contextValue, contextValue.getClass()));
      }
    } else {
      context = null;
    }

    Statement statement = valueFactory.createStatement(subject, predicate, object, context);

    Arrays.stream(statementConsumers)
        .forEach(consumer -> consumer.accept(statement));

    return valueFactory.createStatement(subject, predicate, object, context);
  }

}
