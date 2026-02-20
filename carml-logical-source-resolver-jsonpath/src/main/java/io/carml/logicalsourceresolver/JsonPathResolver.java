package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.NonBlockingParser;
import org.jsfr.json.SurfingConfiguration;
import org.jsfr.json.compiler.JsonPathCompiler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
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
                    String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
        }

        var resolved = resolvedSource
                .getResolved()
                .orElseThrow(() -> new LogicalSourceResolverException(
                        String.format("No source provided for logical sources:%n%s", exception(logicalSources))));

        if (resolved instanceof InputStream resolvedInputStream) {
            return resolveInputStream(resolvedInputStream, logicalSources);
        } else if (resolved instanceof Mono<?> mono) {
            return resolveMono(mono, logicalSources);
        } else if (resolved instanceof JsonNode resolvedJsonNode) {
            return getObjectFlux(resolvedJsonNode, logicalSources);
        }
        throw new LogicalSourceResolverException(String.format(
                "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
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
            return Flux.error(new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources))));
        });
    }

    private Flux<LogicalSourceRecord<JsonNode>> getObjectFlux(
            InputStream inputStream, Charset charset, Set<LogicalSource> logicalSources) {
        try {
            var tmp = IOUtils.toString(inputStream, charset);
            return Flux.fromIterable(logicalSources).flatMap(logicalSource -> readIterator(tmp, logicalSource));
        } catch (IOException e) {
            throw new LogicalSourceResolverException("Error reading input stream.", e);
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
        return Flux.error(new LogicalSourceResolverException(String.format(ERROR_INTERPRETING_RESULT, resultNode)));
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
                    var readStatus = channel.read(byteBuffer);

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
        return jsonNode -> expression -> evaluateExpression(jsonNode, expression);
    }

    /**
     * Validates a JSONPath expression using JSurfer's ANTLR-based parser. Jayway silently accepts gibberish
     * expressions (e.g. "Dhkef;esfkdleshfjdls;fk") as property name lookups and returns null. JSurfer's
     * compiler rejects them with a {@code ParseCancellationException}.
     *
     * <p>Standard JSONPath expressions (starting with "$") are validated directly by JSurfer. Jayway also
     * accepts bare expressions without "$" prefix (e.g. "Name", "@.id", "tags[*]"). These cannot be
     * validated by JSurfer (which requires "$" prefix), so they are checked for characters that are
     * clearly not part of JSONPath syntax or member names (e.g. semicolons).
     */
    private static void validateJsonPathExpression(String expression) {
        if (expression == null || expression.isEmpty()) {
            throw new LogicalSourceResolverException(
                    "Invalid JSONPath expression: expression must not be null or empty");
        }
        if (expression.startsWith("$")) {
            try {
                JsonPathCompiler.compile(expression);
            } catch (Exception e) {
                throw new LogicalSourceResolverException("Invalid JSONPath expression: " + expression, e);
            }
        } else if (containsInvalidBareNameChars(expression)) {
            throw new LogicalSourceResolverException("Invalid JSONPath expression: " + expression);
        }
    }

    private static boolean containsInvalidBareNameChars(String expression) {
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (Character.isLetterOrDigit(c)
                    || c == '_'
                    || c == '-'
                    || c == '.'
                    || c == '['
                    || c == ']'
                    || c == '*'
                    || c == '@'
                    || c == '?'
                    || c == '('
                    || c == ')'
                    || c == '\''
                    || c == '"'
                    || c == ':'
                    || c == '='
                    || c == '!'
                    || c == '&'
                    || c == '|'
                    || c == '>'
                    || c == '<'
                    || c == ' ') {
                continue;
            }
            return true;
        }
        return false;
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

        try {
            if (resultNode.isArray()) {
                return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, List.class));
            }
            if (resultNode.isObject()) {
                return Optional.of(OBJECT_MAPPER.treeToValue(resultNode, Map.class));
            }
            if (resultNode.isValueNode()) {
                var textResult = resultNode.asText();
                if (source.getNulls().contains(textResult)) {
                    return Optional.empty();
                }
                return Optional.of(textResult);
            }

            throw new LogicalSourceResolverException(String.format(ERROR_INTERPRETING_RESULT, resultNode));
        } catch (JsonProcessingException jsonProcessingException) {
            throw new LogicalSourceResolverException(
                    String.format("Error processing expression result %s", resultNode), jsonProcessingException);
        }
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
