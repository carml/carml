package io.carml.engine.function;

import io.carml.engine.RmlMapperException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("java:S1135")
public class Functions {

  private static final Logger LOG = LoggerFactory.getLogger(Functions.class);

  private static final ValueFactory VF = SimpleValueFactory.getInstance();

  private final Map<IRI, ExecuteFunction> fns = new LinkedHashMap<>();

  public Optional<ExecuteFunction> getFunction(IRI iri) {
    return Optional.ofNullable(fns.get(iri));
  }

  public void addFunctions(Object... functions) {
    for (Object fn : functions) {
      Arrays.stream(fn.getClass()
          .getMethods())
          .map(method -> createFunctionExecutor(fn, method))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(function -> fns.put(function.getIri(), function));
    }
  }

  private Optional<ExecuteFunction> createFunctionExecutor(Object obj, Method method) {

    FnoFunction function = method.getAnnotation(FnoFunction.class);
    if (function == null) {
      return Optional.empty();
    }
    var iri = VF.createIRI(function.value());

    List<ExtractParameter> parameterExtractors = Arrays.stream(method.getParameters())
        .map(this::createParameterExtractor)
        .toList();

    LOG.debug("Creating executable FnO function {}", function);
    return Optional.of(new ExecuteFunction() {

      @Override
      public Object execute(Model model, Resource subject, UnaryOperator<Object> returnValueAdapter) {

        List<Object> arguments = parameterExtractors.stream()
            .map(extractor -> extractor.extract(model, subject))
            .toList();

        try {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Executing function {} with arguments {}", method.getName(), arguments);
          }

          Object returnValue = method.invoke(obj, arguments.toArray());

          if (returnValue == null) {
            return null;
          }

          return returnValueAdapter.apply(returnValue);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException exception) {
          throw new RmlMapperException("error executing function", exception);
        }
      }

      @Override
      public IRI getIri() {
        return iri;
      }
    });
  }

  private ExtractParameter createParameterExtractor(Parameter parameter) {

    FnoParam param = parameter.getAnnotation(FnoParam.class);
    if (param == null) {
      throw new RmlMapperException(String.format("no @%s annotation present on parameter", FnoParam.class.getName()));
    }
    var iri = VF.createIRI(param.value());

    Type type = parameter.getType();

    Function<List<Value>, Object> adapter;

    if (type.equals(Integer.TYPE) || type.equals(Integer.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToInt);
    } else if (type.equals(String.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToString);
    } else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToDouble);
    } else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToFloat);
    } else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToLong);
    } else if (type.equals(Boolean.TYPE) || type.equals(Boolean.class)) {
      adapter = values -> singleValueExtraction(values, this::literalToBoolean);
    } else if (Collection.class.isAssignableFrom(parameter.getType())) {
      // TODO: Currently only collections with string parameter type supported.
      adapter = this::collectionValueExtraction;
    } else {
      throw new RmlMapperException(String.format("parameter type [%s] not (yet) supported", type));
    }

    return (model, subject) -> {
      var paramValues = model.filter(subject, iri, null);

      List<Value> values = paramValues.stream()
          .map(Statement::getObject)
          .toList();

      return adapter.apply(values);
    };
  }

  private Object singleValueExtraction(List<Value> values, Function<Value, Object> literalProcessor) {
    if (values == null || values.isEmpty()) {
      // Return null for empty function parameter
      return null;
    }

    expectSingleValue(values);
    return literalProcessor.apply(values.get(0));
  }

  private Object collectionValueExtraction(List<Value> values) {

    if (values == null || values.isEmpty()) {
      // Return null for empty function parameter
      return null;
    }

    return values.stream()
        .map(Value::stringValue)
        .toList();
  }


  public int size() {
    return fns.size();
  }

  private void expectSingleValue(List<Value> values) {
    if (values.size() > 1) {
      throw new IllegalArgumentException(
          String.format("value [%s] has more than one value, which is not expected.", values));
    }
  }

  private String literalToString(Value value) {
    if (value instanceof Literal literal) {
      return literal.stringValue();
    }

    throw new IllegalArgumentException(
        String.format("value [%s] was not a literal, which is expected for a parameter of type String.", value));
  }

  private int literalToInt(Value value) {
    if (value instanceof Literal literal) {
      return literal.intValue();
    }

    throw new IllegalArgumentException(String
        .format("value [%s] was not a literal, which is expected for a parameter of type int or Integer.", value));
  }

  private double literalToDouble(Value value) {
    if (value instanceof Literal literal) {
      return literal.doubleValue();
    }

    throw new IllegalArgumentException(String
        .format("value [%s] was not a literal, which is expected for a parameter of type double or Double.", value));

  }

  private float literalToFloat(Value value) {
    if (value instanceof Literal literal) {
      return literal.floatValue();
    }

    throw new IllegalArgumentException(String
        .format("value [%s] was not a literal, which is expected for a parameter of type float or Float.", value));
  }

  private long literalToLong(Value value) {
    if (value instanceof Literal literal) {
      return literal.longValue();
    }

    throw new IllegalArgumentException(
        String.format("value [%s] was not a literal, which is expected for a parameter of type long or Long.", value));
  }

  private boolean literalToBoolean(Value value) {
    if (value instanceof Literal literal) {
      return literal.booleanValue();
    }


    throw new IllegalArgumentException(String
        .format("value [%s] was not a literal, which is expected for a parameter of type boolean or Boolean.", value));
  }

}
