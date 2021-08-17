package com.taxonic.carml.logical_source_resolver;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.util.LogUtil;
import java.io.InputStream;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonPathResolver implements LogicalSourceResolver<Object> {

  private final JsonSurfer jsonSurfer;

  private static final Configuration JSONPATH_CONF = Configuration.builder()
      .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
      .options(Option.SUPPRESS_EXCEPTIONS)
      .build();

  public static JsonPathResolver getInstance() {
    return new JsonPathResolver(JsonSurferJackson.INSTANCE);
  }

  @Override
  public SourceFlux<Object> getSourceFlux() {
    return this::getObjectFlux;
  }

  private Flux<Object> getObjectFlux(@NonNull Object source, @NonNull LogicalSource logicalSource) {
    if (source instanceof InputStream) {
      return getObjectFlux((InputStream) source, logicalSource);
    } else {
      throw new LogicalSourceResolverException(
          String.format("No valid input stream provided for logical source %s", LogUtil.exception(logicalSource)));
    }
  }

  private Flux<Object> getObjectFlux(InputStream inputStream, LogicalSource logicalSource) {
    return Flux.create(sink -> {
      jsonSurfer.configBuilder()
          .bind(logicalSource.getIterator(), (obj, context) -> sink.next(obj))
          .buildAndSurf(inputStream);
      sink.complete();
    });
  }

  @Override
  public ExpressionEvaluationFactory<Object> getExpressionEvaluationFactory() {
    return object -> expression -> {
      logEvaluateExpression(expression, LOG);

      return Optional.ofNullable(JsonPath.using(JSONPATH_CONF)
          .parse(object.toString())
          .read(expression));
    };
  }

}
