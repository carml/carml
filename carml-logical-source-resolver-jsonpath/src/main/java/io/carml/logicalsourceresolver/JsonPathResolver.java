package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.io.CharStreams;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import io.carml.jsonpath.JsonPathValidationException;
import io.carml.jsonpath.JsonPathValidator;
import io.carml.logicalsourceresolver.sourceresolver.Encodings;
import io.carml.model.JsonPathReferenceFormulation;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.NonBlockingParser;
import org.jsfr.json.SurfingConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class JsonPathResolver implements LogicalSourceResolver<JsonNode> {

    public static final String NAME = "JsonPathResolver";

    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String ERROR_INTERPRETING_RESULT = "Error interpreting expression result %s";

    private static final Configuration JSONPATH_CONF = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
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

    private JsonPathResolver(Source source, JsonSurfer jsonSurfer, int bufferSize) {
        this.source = Objects.requireNonNull(source, "source");
        this.jsonSurfer = Objects.requireNonNull(jsonSurfer, "jsonSurfer");
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive");
        }
        this.bufferSize = bufferSize;
    }

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

        if (resolvedSource == null) {
            throw new LogicalSourceResolverException(
                    "No source provided for logical sources:%n%s".formatted(exception(logicalSources)));
        }

        var resolved = resolvedSource
                .getResolved()
                .orElseThrow(() -> new LogicalSourceResolverException(
                        "No source provided for logical sources:%n%s".formatted(exception(logicalSources))));

        if (resolved instanceof InputStream resolvedInputStream) {
            return resolveInputStream(resolvedInputStream, logicalSources);
        } else if (resolved instanceof Mono<?> mono) {
            return resolveMono(mono, logicalSources);
        } else if (resolved instanceof JsonNode resolvedJsonNode) {
            return getObjectFlux(resolvedJsonNode, logicalSources);
        }
        throw new LogicalSourceResolverException(
                "Unsupported source object provided for logical sources:%n%s".formatted(exception(logicalSources)));
    }

    private Flux<LogicalSourceRecord<JsonNode>> resolveInputStream(
            InputStream inputStream, Set<LogicalSource> logicalSources) {
        var charset = Encodings.resolveCharset(source.getEncoding()).orElse(UTF_8);
        if (charset == UTF_8) {
            return getObjectFlux(inputStream, logicalSources);
        }
        return getObjectFlux(inputStream, charset, logicalSources);
    }

    private Flux<LogicalSourceRecord<JsonNode>> resolveMono(Mono<?> mono, Set<LogicalSource> logicalSources) {
        return mono.flatMapMany(resolvedMono -> {
            if (resolvedMono instanceof InputStream resolvedInputStreamMono) {
                return resolveInputStream(resolvedInputStreamMono, logicalSources);
            }
            return Flux.error(
                    new LogicalSourceResolverException("Unsupported source object provided for logical sources:%n%s"
                            .formatted(exception(logicalSources))));
        });
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(
            InputStream inputStream, Charset charset, Set<LogicalSource> logicalSources) {
        try {
            var tmp = CharStreams.toString(new InputStreamReader(inputStream, charset));
            return Flux.fromIterable(logicalSources).flatMap(logicalSource -> readIterator(tmp, logicalSource));
        } catch (IOException e) {
            throw new LogicalSourceResolverException("Error reading input stream.", e);
        }
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(
            InputStream inputStream, Set<LogicalSource> logicalSources) {
        return PausableFluxBridge.<LogicalSourceRecord<JsonNode>>builder()
                .sourceFactory(emitter -> {
                    var configBuilder = jsonSurfer.configBuilder();

                    bridgeAndListen(logicalSources, configBuilder, emitter);

                    var config = configBuilder.build();
                    var parser = jsonSurfer.createNonBlockingParser(config);

                    return new JsonPausableSource(inputStream, parser, emitter, bufferSize);
                })
                .onDispose(() -> {
                    try {
                        inputStream.close();
                    } catch (IOException ioException) {
                        throw new LogicalSourceResolverException("Error closing input stream.", ioException);
                    }
                })
                .flux();
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(JsonNode jsonNode, Set<LogicalSource> logicalSources) {
        return Flux.fromIterable(logicalSources).flatMap(logicalSource -> readIterator(jsonNode, logicalSource));
    }

    private static Flux<LogicalSourceRecord<JsonNode>> readIterator(Object input, LogicalSource logicalSource) {
        try {
            var resultNode =
                    JsonPath.using(JSONPATH_CONF).parse(input).read(logicalSource.getIterator(), JsonNode.class);
            return toLogicalSourceRecordFlux(resultNode, logicalSource);
        } catch (PathNotFoundException pathNotFoundException) {
            return Flux.empty();
        } catch (InvalidPathException invalidPathException) {
            return Flux.error(new LogicalSourceResolverException(
                    "Invalid JSONPath expression in iterator: " + logicalSource.getIterator(), invalidPathException));
        }
    }

    private static Flux<LogicalSourceRecord<JsonNode>> toLogicalSourceRecordFlux(
            JsonNode resultNode, LogicalSource logicalSource) {
        if (resultNode == null || resultNode.isNull()) {
            return Flux.empty();
        }
        if (resultNode.isArray()) {
            return Flux.fromStream(StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(resultNode.elements(), Spliterator.ORDERED), false))
                    .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
        }
        if (resultNode.isObject() || resultNode.isValueNode()) {
            return Flux.just(LogicalSourceRecord.of(logicalSource, resultNode));
        }
        return Flux.error(new LogicalSourceResolverException(ERROR_INTERPRETING_RESULT.formatted(resultNode)));
    }

    private void bridgeAndListen(
            Set<LogicalSource> logicalSources,
            SurfingConfiguration.Builder configBuilder,
            PausableFluxBridge.Emitter<LogicalSourceRecord<JsonNode>> emitter) {
        logicalSources.forEach(logicalSource -> {
            try {
                configBuilder.bind(logicalSource.getIterator(), (value, context) -> {
                    if (!(value instanceof JsonNode)) {
                        throw new LogicalSourceResolverException("Encountered non-JsonNode value: %s".formatted(value));
                    }
                    emitter.next(LogicalSourceRecord.of(logicalSource, (JsonNode) value));
                });
            } catch (RuntimeException parsingException) {
                emitter.error(new LogicalSourceResolverException(
                        "An exception occurred while parsing expression: %s".formatted(logicalSource.getIterator())));
            }
        });
    }

    private static class JsonPausableSource implements PausableSource {
        private final NonBlockingParser parser;
        private final PausableFluxBridge.Emitter<LogicalSourceRecord<JsonNode>> emitter;
        private final ReadableByteChannel channel;
        private final ByteBuffer byteBuffer;

        private volatile boolean paused;
        private volatile boolean completed;

        JsonPausableSource(
                InputStream inputStream,
                NonBlockingParser parser,
                PausableFluxBridge.Emitter<LogicalSourceRecord<JsonNode>> emitter,
                int bufferSize) {
            this.parser = parser;
            this.emitter = emitter;
            this.channel = Channels.newChannel(inputStream);
            this.byteBuffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        public void start() {
            readLoop();
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
            readLoop();
        }

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        private void readLoop() {
            try {
                while (!completed && !paused) {
                    var readStatus = channel.read(byteBuffer);

                    if (readStatus == -1) {
                        parser.endOfInput();
                        completed = true;
                        closeChannel();
                        emitter.complete();
                        break;
                    }

                    parser.feed(byteBuffer.array(), 0, byteBuffer.position());
                    byteBuffer.clear();
                }
            } catch (IOException ioException) {
                closeChannel();
                emitter.error(new LogicalSourceResolverException("Error reading input stream.", ioException));
            }
        }

        private void closeChannel() {
            try {
                channel.close();
            } catch (IOException ignored) {
                // Channel close failure is non-critical; the input stream is closed via onDispose.
            }
        }
    }

    @Override
    public ExpressionEvaluationFactory<JsonNode> getExpressionEvaluationFactory() {
        return jsonNode -> expression -> evaluateExpression(jsonNode, expression);
    }

    /**
     * Validates a JSONPath expression, wrapping any {@link JsonPathValidationException} from the
     * shared validator as a {@link LogicalSourceResolverException}.
     */
    private static void validateJsonPathExpression(String expression) {
        try {
            JsonPathValidator.validate(expression);
        } catch (JsonPathValidationException e) {
            throw new LogicalSourceResolverException(e.getMessage(), e.getCause());
        }
    }

    private static Optional<JsonNode> readExpression(JsonNode jsonNode, String expression) {
        validateJsonPathExpression(expression);

        JsonNode resultNode;
        try {
            resultNode = JsonPath.using(JSONPATH_CONF).parse(jsonNode).read(expression, JsonNode.class);
        } catch (PathNotFoundException pathNotFoundException) {
            return Optional.empty();
        } catch (InvalidPathException invalidPathException) {
            throw new LogicalSourceResolverException(
                    "Invalid JSONPath expression: " + expression, invalidPathException);
        }

        if (resultNode == null || resultNode.isNull()) {
            return Optional.empty();
        }
        return Optional.of(resultNode);
    }

    private Optional<Object> evaluateExpression(JsonNode jsonNode, String expression) {
        logEvaluateExpression(expression, LOG);

        var resultNodeOpt = readExpression(jsonNode, expression);
        if (resultNodeOpt.isEmpty()) {
            return Optional.empty();
        }
        var resultNode = resultNodeOpt.get();

        if (resultNode.isArray()) {
            if (JsonPath.compile(expression).isDefinite()) {
                throw new LogicalSourceResolverException(("JSONPath expression '%s' evaluated to an array,"
                                + " but only scalar values are allowed."
                                + " Use an iterator to process array data.")
                        .formatted(expression));
            }
            return Optional.of(StreamSupport.stream(resultNode.spliterator(), false)
                    .filter(node -> !node.isNull())
                    .map(node -> {
                        if (node.isValueNode()) {
                            var text = node.asText();
                            if (source.getNulls().contains(text)) {
                                return null;
                            }
                            return text;
                        }
                        // Return raw JsonNode for objects/arrays — needed for iterable field evaluation
                        return node;
                    })
                    .filter(Objects::nonNull)
                    .toList());
        }
        if (resultNode.isObject()) {
            // Return raw JsonNode — needed for iterable field evaluation where the iterator
            // expression targets a single nested object (e.g. "$.measures" → {"weight": 1500})
            return Optional.of(resultNode);
        }
        if (resultNode.isValueNode()) {
            var textResult = resultNode.asText();
            if (source.getNulls().contains(textResult)) {
                return Optional.empty();
            }
            return Optional.of(textResult);
        }

        throw new LogicalSourceResolverException(ERROR_INTERPRETING_RESULT.formatted(resultNode));
    }

    @Override
    public Optional<Function<String, List<JsonNode>>> getInlineRecordParser() {
        return Optional.of(text -> {
            try {
                return List.of(OBJECT_MAPPER.readTree(text));
            } catch (IOException e) {
                throw new LogicalSourceResolverException(
                        "Error parsing inline JSON text for iterable field evaluation", e);
            }
        });
    }

    // Note: unlike getExpressionEvaluationFactory(), this does not check source.getNulls() —
    // the datatype is inferred from the JSON node type, not the string value. A field whose text
    // matches a null sentinel still has a valid JSON type for datatype inference.
    @Override
    public Optional<DatatypeMapperFactory<JsonNode>> getDatatypeMapperFactory() {
        return Optional.of(jsonNode -> expression -> resolveDatatype(jsonNode, expression));
    }

    private static Optional<IRI> resolveDatatype(JsonNode jsonNode, String expression) {
        return readExpression(jsonNode, expression).flatMap(resultNode -> {
            // DatatypeMapper returns a single type per expression; for arrays, use the first
            // non-null element's type. JSON arrays are typically homogeneous in RML sources.
            if (resultNode.isArray()) {
                return getFirstNonNullDatatype(resultNode);
            }
            return getJsonNodeXsdDatatype(resultNode);
        });
    }

    private static Optional<IRI> getFirstNonNullDatatype(JsonNode arrayNode) {
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false)
                .filter(element -> !element.isNull())
                .findFirst()
                .flatMap(JsonPathResolver::getJsonNodeXsdDatatype);
    }

    private static Optional<IRI> getJsonNodeXsdDatatype(JsonNode node) {
        if (node.isIntegralNumber()) {
            return Optional.of(XSD.INTEGER);
        }
        if (node.isFloatingPointNumber()) {
            return Optional.of(XSD.DOUBLE);
        }
        if (node.isBoolean()) {
            return Optional.of(XSD.BOOLEAN);
        }
        return Optional.empty();
    }

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    @SuppressWarnings("unused")
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
