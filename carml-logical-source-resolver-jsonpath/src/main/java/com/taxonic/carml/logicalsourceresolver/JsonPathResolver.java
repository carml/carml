package com.taxonic.carml.logicalsourceresolver;

import static com.taxonic.carml.util.LogUtil.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.taxonic.carml.model.LogicalSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.NonBlockingParser;
import org.jsfr.json.SurfingConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonPathResolver implements LogicalSourceResolver<JsonNode> {

  private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Configuration JSONPATH_CONF = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
      .options(Option.SUPPRESS_EXCEPTIONS)
      .build();

  private final JsonSurfer jsonSurfer;

  private final int bufferSize;

  public static JsonPathResolver getInstance() {
    return getInstance(DEFAULT_BUFFER_SIZE);
  }

  public static JsonPathResolver getInstance(int bufferSize) {
    return new JsonPathResolver(JsonSurferJackson.INSTANCE, bufferSize);
  }

  @Override
  public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<JsonNode>>> getLogicalSourceRecords(
      Set<LogicalSource> logicalSources) {
    return resolvedSource -> getLogicalSourceRecordFlux(resolvedSource, logicalSources);
  }

  @SuppressWarnings("java:S3655")
  private Flux<LogicalSourceRecord<JsonNode>> getLogicalSourceRecordFlux(ResolvedSource<?> resolvedSource,
      Set<LogicalSource> logicalSources) {
    if (resolvedSource == null || !(resolvedSource.getResolved()
        .isPresent()
        && resolvedSource.getResolved()
            .get() instanceof InputStream)) {
      throw new LogicalSourceResolverException(
          String.format("No valid input stream provided for logical sources:%n%s", exception(logicalSources)));
    }

    return getObjectFlux((InputStream) resolvedSource.getResolved()
        .get(), logicalSources);
  }

  private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(InputStream inputStream,
      Set<LogicalSource> logicalSources) {
    if (logicalSources.isEmpty()) {
      throw new IllegalStateException("No logical sources registered");
    }

    var outstandingRequests = new AtomicLong();
    var sourcePaused = new AtomicBoolean();
    var sourceCompleted = new AtomicBoolean();

    return Flux.create(sink -> {
      sink.onRequest(requested -> {
        var outstanding = outstandingRequests.addAndGet(requested);
        if (sourcePaused.get() && outstanding >= 0L) {
          sourcePaused.compareAndSet(true, false);
        } else if (!sourcePaused.get() && outstanding < 0L) {
          sourcePaused.compareAndSet(false, true);
        }
      });
      sink.onDispose(() -> {
        try {
          inputStream.close();
        } catch (IOException ioException) {
          throw new LogicalSourceResolverException("Error closing input stream.", ioException);
        }
      });

      var configBuilder = jsonSurfer.configBuilder();

      bridgeAndListen(logicalSources, configBuilder, sink, outstandingRequests);

      var config = configBuilder.build();
      var parser = jsonSurfer.createNonBlockingParser(config);

      readSource(inputStream, parser, sink, sourcePaused, sourceCompleted);
    });
  }

  private void bridgeAndListen(Set<LogicalSource> logicalSources, SurfingConfiguration.Builder configBuilder,
      FluxSink<LogicalSourceRecord<JsonNode>> sink, AtomicLong outstandingRequests) {
    logicalSources.forEach(logicalSource -> {
      try {
        configBuilder.bind(logicalSource.getIterator(), (value, context) -> {
          if (!(value instanceof JsonNode)) {
            throw new LogicalSourceResolverException(String.format("Encountered non-JsonNode value: %s", value));
          }
          sink.next(LogicalSourceRecord.of(logicalSource, (JsonNode) value));
          outstandingRequests.decrementAndGet();
        });
      } catch (RuntimeException parsingException) {
        sink.error(new LogicalSourceResolverException(
            String.format("An exception occurred while parsing expression: %s", logicalSource.getIterator())));
      }
    });
  }

  private void readSource(InputStream inputStream, NonBlockingParser parser,
      FluxSink<LogicalSourceRecord<JsonNode>> sink, AtomicBoolean parsingPaused, AtomicBoolean parsingCompleted) {
    try (var channel = Channels.newChannel(inputStream)) {
      var byteBuffer = ByteBuffer.allocate(bufferSize);

      while (!parsingCompleted.get()) {
        while (!parsingPaused.get()) {
          var readStatus = inputStream.available() > 0 ? channel.read(byteBuffer) : -1;

          if (readStatus == -1) {
            parser.endOfInput();
            parsingCompleted.compareAndSet(false, true);
            sink.complete();
            break;
          }

          parser.feed(byteBuffer.array(), 0, byteBuffer.position());
          byteBuffer.clear();
        }
      }
    } catch (IOException ioException) {
      sink.error(new LogicalSourceResolverException("Error reading input stream.", ioException));
    }
  }

  @Override
  public ExpressionEvaluationFactory<JsonNode> getExpressionEvaluationFactory() {
    return jsonNode -> expression -> {
      logEvaluateExpression(expression, LOG);

      var resultNode = JsonPath.using(JSONPATH_CONF)
          .parse(jsonNode)
          .read(expression, JsonNode.class);

      try {
        if (resultNode.isNull()) {
          return Optional.empty();
        }
        if (resultNode.isArray()) {
          return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, List.class));
        } else if (resultNode.isObject()) {
          return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, Map.class));
        } else if (resultNode.isValueNode()) {
          return Optional.of(resultNode.asText());
        }

        throw new LogicalSourceResolverException(String.format("Error interpreting expression result %s", resultNode));
      } catch (JsonProcessingException jsonProcessingException) {
        throw new LogicalSourceResolverException(String.format("Error processing expression result %s", resultNode),
            jsonProcessingException);
      }
    };
  }
}
