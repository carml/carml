package com.taxonic.carml.util;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
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
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;

public final class Models {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  private Models() {}

  public static Model parseTrig(String resource) {
    return parse(resource, RDFFormat.TRIG);
  }

  public static Model parse(String resource) {
    return parse(resource, RDFFormat.TURTLE);
  }

  public static Model parse(String resource, RDFFormat format) {
    try (InputStream input = Models.class.getClassLoader()
        .getResourceAsStream(resource)) {
      return parse(input, format);
    } catch (IOException ioException) {
      throw new ModelsException(String.format("failed to parse resource [%s] as [%s]", resource, format), ioException);
    }
  }

  public static Model parse(InputStream inputStream, RDFFormat format) {
    try (InputStream is = inputStream) {
      ParserConfig settings = new ParserConfig();
      settings.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
      return Rio.parse(is, "http://none.com/", format, settings, SimpleValueFactory.getInstance(),
          new ParseErrorLogger());
    } catch (IOException ioException) {
      throw new ModelsException(String.format("failed to parse input stream [%s] as [%s]", inputStream, format),
          ioException);
    }
  }

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
      throw new ModelsException(
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
      throw new ModelsException(String.format("Expected subjectValue `%s` to be instance of Resource, but was %s",
          subjectValue, subjectValue.getClass()));
    }

    IRI predicate;
    if (predicateValue instanceof Resource) {
      predicate = (IRI) predicateValue;
    } else {
      throw new ModelsException(String.format("Expected predicateValue `%s` to be instance of IRI, but was %s",
          predicateValue, predicateValue.getClass()));
    }

    Resource context;
    if (contextValue != null) {
      if (contextValue instanceof Resource) {
        context = contextModifier.apply((Resource) contextValue);
      } else {
        throw new ModelsException(String.format("Expected contextValue `%s` to be instance of Resource, but was %s",
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
