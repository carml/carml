package com.taxonic.carml.logicalsourceresolver;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.util.LogUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JaywayJacksonJsonPathResolver implements LogicalSourceResolver<Object> {

  private static final Configuration JSONPATH_JSON_NODE_CONF = Configuration.builder()
      .jsonProvider(new JacksonJsonProvider())
      .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
      .options(Option.SUPPRESS_EXCEPTIONS)
      .build();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static JaywayJacksonJsonPathResolver getInstance() {
    return new JaywayJacksonJsonPathResolver();
  }

  @Override
  public SourceFlux<Object> getSourceFlux() {
    return this::getObjectFlux;
  }

  @SuppressWarnings("unchecked")
  private Flux<Object> getObjectFlux(@NonNull Object source, @NonNull LogicalSource logicalSource) {
    if (source instanceof InputStream) {
      return getObjectFlux((InputStream) source, logicalSource);
    } else if (source instanceof Map) {
      return getObjectFluxFromCustomObject((Map<String, Object>) source, logicalSource);
    } else {
      throw new LogicalSourceResolverException(
          String.format("No supported source object provided for logical source %s", LogUtil.exception(logicalSource)));
    }
  }

  private Flux<Object> getObjectFlux(InputStream inputStream, LogicalSource logicalSource) {
    try {
      return getObjectFluxFromCustomObject(OBJECT_MAPPER.readValue(inputStream, new TypeReference<>() {}),
          logicalSource);
    } catch (IOException ioException) {
      throw new LogicalSourceResolverException("Error reading input stream", ioException);
    }
  }

  private Flux<Object> getObjectFluxFromCustomObject(Map<String, Object> customObject, LogicalSource logicalSource) {
    Object data;
    try {
      data = JsonPath.using(JSONPATH_JSON_NODE_CONF)
          .parse(customObject)
          .read(logicalSource.getIterator());
    } catch (RuntimeException exception) {
      throw new LogicalSourceResolverException(
          String.format("An exception occurred while evaluating: %s", logicalSource.getIterator()));
    }

    if (data == null) {
      return Flux.empty();
    }

    boolean isCollection = Collection.class.isAssignableFrom(data.getClass());
    return isCollection ? Flux.fromIterable((Collection<?>) data) : Flux.just(data);
  }

  @Override
  public ExpressionEvaluationFactory<Object> getExpressionEvaluationFactory() {
    return object -> expression -> {
      logEvaluateExpression(expression, LOG);

      try {
        return Optional.ofNullable(JsonPath.using(JSONPATH_JSON_NODE_CONF)
            .parse(object)
            .read(expression));
      } catch (RuntimeException exception) {
        throw new LogicalSourceResolverException(
            String.format("An exception occurred while evaluating: %s", expression));
      }
    };
  }

}
