package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import io.carml.logicalsourceresolver.sourceresolver.Encodings;
import io.carml.model.JsonPathReferenceFormulation;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.NonBlockingParser;
import org.jsfr.json.SurfingConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonPathResolver implements LogicalSourceResolver<JsonNode> {

    public static final String NAME = "JsonPathResolver";

    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Configuration JSONPATH_CONF = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .options(Option.SUPPRESS_EXCEPTIONS)
            .build();

    public static LogicalSourceResolverFactory<JsonNode> factory() {
        return factory(DEFAULT_BUFFER_SIZE);
    }

    public static LogicalSourceResolverFactory<JsonNode> factory(int bufferSize) {
        return source -> new JsonPathResolver(source, JsonSurferJackson.INSTANCE, bufferSize);
    }

    private final Source source;

    private final JsonSurfer jsonSurfer;

    private final int bufferSize;

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<JsonNode>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources) {
        return resolvedSource -> getLogicalSourceRecordFlux(resolvedSource, logicalSources);
    }

    private Flux<LogicalSourceRecord<JsonNode>> getLogicalSourceRecordFlux(
            ResolvedSource<?> resolvedSource, Set<LogicalSource> logicalSources) {
        if (logicalSources.isEmpty()) {
            throw new IllegalStateException("No logical sources registered");
        }

        if (resolvedSource == null || resolvedSource.getResolved().isEmpty()) {
            throw new LogicalSourceResolverException(
                    String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
        }

        var resolved = resolvedSource.getResolved().get();

        if (resolved instanceof InputStream resolvedInputStream) {
            var charset = Encodings.resolveCharset(source.getEncoding()).orElse(UTF_8);

            if (charset == UTF_8) {
                return getObjectFlux(resolvedInputStream, logicalSources);
            } else {
                return getObjectFlux(resolvedInputStream, charset, logicalSources);
            }
        } else if (resolved instanceof Mono<?> mono) {
            return mono.flatMapMany(resolvedMono -> {
                if (resolvedMono instanceof InputStream resolvedInputStreamMono) {
                    var charset = Encodings.resolveCharset(source.getEncoding()).orElse(UTF_8);

                    if (charset == UTF_8) {
                        return getObjectFlux(resolvedInputStreamMono, logicalSources);
                    } else {
                        return getObjectFlux(resolvedInputStreamMono, charset, logicalSources);
                    }
                } else {
                    throw new LogicalSourceResolverException(String.format(
                            "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
                }
            });
        } else if (resolved instanceof JsonNode resolvedJsonNode) {
            return getObjectFlux(resolvedJsonNode, logicalSources);
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(
            InputStream inputStream, Charset charset, Set<LogicalSource> logicalSources) {
        try {
            var tmp = IOUtils.toString(inputStream, charset);
            return Flux.fromIterable(logicalSources).flatMap(logicalSource -> {
                var resultNode =
                        JsonPath.using(JSONPATH_CONF).parse(tmp).read(logicalSource.getIterator(), JsonNode.class);

                if (resultNode == null || resultNode.isNull()) {
                    return Flux.empty();
                }
                if (resultNode.isArray()) {
                    return Flux.fromStream(StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(resultNode.elements(), Spliterator.ORDERED),
                                    false))
                            .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
                } else if (resultNode.isObject() || resultNode.isValueNode()) {
                    return Flux.just(LogicalSourceRecord.of(logicalSource, resultNode));
                }
                throw new LogicalSourceResolverException(
                        String.format("Error interpreting expression result %s", resultNode));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(
            InputStream inputStream, Set<LogicalSource> logicalSources) {
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

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(JsonNode jsonNode, Set<LogicalSource> logicalSources) {
        return Flux.fromIterable(logicalSources)
                .flatMap(logicalSource -> getObjectFluxForLogicalSource(jsonNode, logicalSource));
    }

    private void bridgeAndListen(
            Set<LogicalSource> logicalSources,
            SurfingConfiguration.Builder configBuilder,
            FluxSink<LogicalSourceRecord<JsonNode>> sink,
            AtomicLong outstandingRequests) {
        logicalSources.forEach(logicalSource -> {
            try {
                configBuilder.bind(logicalSource.getIterator(), (value, context) -> {
                    if (!(value instanceof JsonNode)) {
                        throw new LogicalSourceResolverException(
                                String.format("Encountered non-JsonNode value: %s", value));
                    }
                    sink.next(LogicalSourceRecord.of(logicalSource, (JsonNode) value));
                    outstandingRequests.decrementAndGet();
                });
            } catch (RuntimeException parsingException) {
                sink.error(new LogicalSourceResolverException(String.format(
                        "An exception occurred while parsing expression: %s", logicalSource.getIterator())));
            }
        });
    }

    private void readSource(
            InputStream inputStream,
            NonBlockingParser parser,
            FluxSink<LogicalSourceRecord<JsonNode>> sink,
            AtomicBoolean parsingPaused,
            AtomicBoolean parsingCompleted) {
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

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFluxForLogicalSource(
            JsonNode jsonNode, LogicalSource logicalSource) {
        var resultNode =
                JsonPath.using(JSONPATH_CONF).parse(jsonNode).read(logicalSource.getIterator(), JsonNode.class);

        if (resultNode == null || resultNode.isNull()) {
            return Flux.empty();
        }
        if (resultNode.isArray()) {
            return Flux.fromStream(StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(resultNode.elements(), Spliterator.ORDERED), false))
                    .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
        } else if (resultNode.isObject() || resultNode.isValueNode()) {
            return Flux.just(LogicalSourceRecord.of(logicalSource, resultNode));
        }
        throw new LogicalSourceResolverException(String.format("Error interpreting expression result %s", resultNode));
    }

    @Override
    public ExpressionEvaluationFactory<JsonNode> getExpressionEvaluationFactory() {
        return jsonNode -> expression -> {
            logEvaluateExpression(expression, LOG);

            var resultNode = JsonPath.using(JSONPATH_CONF).parse(jsonNode).read(expression, JsonNode.class);

            try {
                if (resultNode == null || resultNode.isNull()) {
                    return Optional.empty();
                }
                if (resultNode.isArray()) {
                    return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, List.class));
                } else if (resultNode.isObject()) {
                    return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, Map.class));
                } else if (resultNode.isValueNode()) {
                    var textResult = resultNode.asText();
                    if (source.getNulls().contains(textResult)) {
                        return Optional.empty();
                    }

                    return Optional.of(textResult);
                }

                throw new LogicalSourceResolverException(
                        String.format("Error interpreting expression result %s", resultNode));
            } catch (JsonProcessingException jsonProcessingException) {
                throw new LogicalSourceResolverException(
                        String.format("Error processing expression result %s", resultNode), jsonProcessingException);
            }
        };
    }

    @Override
    public Optional<DatatypeMapperFactory<JsonNode>> getDatatypeMapperFactory() {
        return Optional.empty();
    }

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    public static class Matcher implements MatchingLogicalSourceResolverFactory {

        @Override
        public Optional<MatchedLogicalSourceResolverFactory> apply(LogicalSource logicalSource) {
            var scoreBuilder = MatchedLogicalSourceResolverFactory.MatchScore.builder();

            if (matchesReferenceFormulation(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, JsonPathResolver.factory()));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() instanceof JsonPathReferenceFormulation;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
