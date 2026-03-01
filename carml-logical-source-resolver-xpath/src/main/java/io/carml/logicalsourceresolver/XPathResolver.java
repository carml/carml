package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;

import com.google.auto.service.AutoService;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.XPathReferenceFormulation;
import io.carml.model.XmlSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.xml.xpath.XPathException;
import jlibs.xml.DefaultNamespaceContext;
import jlibs.xml.sax.dog.NodeItem;
import jlibs.xml.sax.dog.XMLDog;
import jlibs.xml.sax.dog.expr.Expression;
import jlibs.xml.sax.dog.expr.InstantEvaluationListener;
import jlibs.xml.sax.dog.sniff.DOMBuilder;
import jlibs.xml.sax.dog.sniff.Event;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmValue;
import org.jaxen.saxpath.SAXPathException;
import org.xml.sax.InputSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class XPathResolver implements LogicalSourceResolver<XdmItem> {

    public static final String NAME = "XPathResolver";

    public static LogicalSourceResolverFactory<XdmItem> factory() {
        return factory(true);
    }

    public static LogicalSourceResolverFactory<XdmItem> factory(boolean autoNodeTextExtraction) {
        var processor = new Processor(false);
        var compiler = processor.newXPathCompiler();
        compiler.setCaching(true);
        return factory(processor, compiler, autoNodeTextExtraction);
    }

    public static LogicalSourceResolverFactory<XdmItem> factory(
            Processor xpathProcessor, XPathCompiler xpathCompiler, boolean autoNodeTextExtraction) {
        return source -> new XPathResolver(
                source,
                new DefaultNamespaceContext(),
                xpathProcessor,
                xpathCompiler,
                autoNodeTextExtraction,
                new HashMap<>());
    }

    private final Source source;

    private final DefaultNamespaceContext nsContext;

    private final Processor xpathProcessor;

    private final XPathCompiler xpathCompiler;

    private final boolean autoNodeTextExtraction;

    private final Map<Set<LogicalSource>, XMLDog> xmlDogCache;

    private void setNamespaces(LogicalSource logicalSource) {
        if (source instanceof XmlSource xmlSource) {
            xmlSource.getDeclaredNamespaces().forEach(n -> {
                nsContext.declarePrefix(n.getPrefix(), n.getName());
                xpathCompiler.declareNamespace(n.getPrefix(), n.getName());
            });
        }

        if (logicalSource.getReferenceFormulation() instanceof XPathReferenceFormulation xpathReferenceFormulation) {
            xpathReferenceFormulation.getNamespaces().forEach(namespace -> {
                nsContext.declarePrefix(namespace.getNamespacePrefix(), namespace.getNamespaceUrl());
                xpathCompiler.declareNamespace(namespace.getNamespacePrefix(), namespace.getNamespaceUrl());
            });
        }
    }

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<XdmItem>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources) {
        return resolvedSource -> getLogicalSourceRecordFlux(resolvedSource, logicalSources);
    }

    private Flux<LogicalSourceRecord<XdmItem>> getLogicalSourceRecordFlux(
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
            return getXpathResultFlux(resolvedInputStream, logicalSources);
        } else if (resolved instanceof Mono<?> mono) {
            return mono.flatMapMany(resolvedMono -> {
                if (resolvedMono instanceof InputStream resolvedInputStreamMono) {
                    return getXpathResultFlux(resolvedInputStreamMono, logicalSources);
                } else {
                    throw new LogicalSourceResolverException(String.format(
                            "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
                }
            });
        } else if (resolved instanceof XdmItem resolvedXdmItem) {
            return getXpathResultFlux(resolvedXdmItem, logicalSources);
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(
            InputStream inputStream, Set<LogicalSource> logicalSources) {
        return PausableFluxBridge.<LogicalSourceRecord<XdmItem>>builder()
                .sourceFactory(emitter -> {
                    var pausableReader = new PausableStaxXmlReader();

                    Map<String, LogicalSource> logicalSourceByExpression = new HashMap<>();
                    XMLDog xmlDog = prepareXmlDog(emitter, logicalSources, logicalSourceByExpression);
                    var event = xmlDog.createEvent();
                    bridgeAndListen(logicalSourceByExpression, event, emitter);

                    return new XPathPausableSource(pausableReader, xmlDog, event, inputStream);
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

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(XdmItem xdmItem, Set<LogicalSource> logicalSources) {
        return Flux.fromIterable(logicalSources)
                .flatMap(logicalSource -> getXpathResultFluxForLogicalSource(xdmItem, logicalSource));
    }

    private XMLDog prepareXmlDog(
            PausableFluxBridge.Emitter<LogicalSourceRecord<XdmItem>> emitter,
            Set<LogicalSource> logicalSources,
            Map<String, LogicalSource> logicalSourceByExpression) {
        XMLDog xmlDog;
        if (xmlDogCache.containsKey(logicalSources)) {
            xmlDog = xmlDogCache.get(logicalSources);
            logicalSources.forEach(
                    logicalSource -> logicalSourceByExpression.put(logicalSource.getIterator(), logicalSource));
        } else {
            xmlDog = new XMLDog(nsContext);
            logicalSources.forEach(logicalSource -> {
                setNamespaces(logicalSource);
                try {
                    var expression = logicalSource.getIterator();
                    xmlDog.addXPath(expression);
                    logicalSourceByExpression.put(expression, logicalSource);
                } catch (SAXPathException saxPathException) {
                    emitter.error(new LogicalSourceResolverException(
                            "Error parsing XPath expression: %s".formatted(logicalSource.getIterator()),
                            saxPathException));
                }
            });

            xmlDogCache.put(logicalSources, xmlDog);
        }

        return xmlDog;
    }

    private void bridgeAndListen(
            Map<String, LogicalSource> logicalSourceByExpression,
            Event event,
            PausableFluxBridge.Emitter<LogicalSourceRecord<XdmItem>> emitter) {

        var expressionCompletion = logicalSourceByExpression.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> false));

        event.setXMLBuilder(new DOMBuilder());
        event.setListener(new InstantEvaluationListener() {
            private final DocumentBuilder docBuilder = xpathProcessor.newDocumentBuilder();

            @Override
            public void onNodeHit(Expression expression, NodeItem nodeItem) {
                var logicalSource = logicalSourceByExpression.get(expression.getXPath());
                emitter.next(LogicalSourceRecord.of(logicalSource, (XdmItem) docBuilder.wrap(nodeItem.xml)));
            }

            @Override
            public void finishedNodeSet(Expression expression) {
                expressionCompletion.put(expression.getXPath(), true);
                if (expressionCompletion.values().stream().allMatch(Boolean::valueOf)) {
                    emitter.complete();
                }
            }

            @Override
            public void onResult(Expression expression, Object object) {
                var logicalSource = logicalSourceByExpression.get(expression.getXPath());
                emitter.next(LogicalSourceRecord.of(logicalSource, (XdmItem) docBuilder.wrap(object)));
            }
        });
    }

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFluxForLogicalSource(
            XdmItem xdmItem, LogicalSource logicalSource) {
        try {
            var selector = xpathCompiler.compile(logicalSource.getIterator()).load();
            selector.setContextItem(xdmItem);
            var value = selector.evaluate();

            if (value.isEmptySequence()) {
                return Flux.empty();
            }

            return Flux.fromIterable(value).map(item -> LogicalSourceRecord.of(logicalSource, item));
        } catch (SaxonApiException e) {
            throw new LogicalSourceResolverException(
                    "Error applying XPath expression [%s] to entry [%s]"
                            .formatted(logicalSource.getIterator(), xdmItem),
                    e);
        }
    }

    @Override
    public ExpressionEvaluationFactory<XdmItem> getExpressionEvaluationFactory() {
        return entry -> expression -> {
            logEvaluateExpression(expression, LOG);

            try {
                var selector = xpathCompiler.compile(expression).load();
                selector.setContextItem(entry);
                var value = selector.evaluate();

                if (value.size() > 1) {
                    throw new LogicalSourceResolverException(
                            "XPath expression '%s' evaluated to multiple items, but only scalar values are allowed. Use an iterator to process multiple items."
                                    .formatted(expression));
                } else if (value.isEmptySequence()) {
                    return Optional.empty();
                }

                var item = value.itemAt(0);
                return Optional.ofNullable(getItemStringValue(item, value));
            } catch (SaxonApiException e) {
                throw new LogicalSourceResolverException(
                        "Error applying XPath expression [%s] to entry [%s]".formatted(expression, entry), e);
            }
        };
    }

    private String getItemStringValue(XdmItem item, XdmValue value) {
        var stringValue = item.getStringValue();
        if (source.getNulls().contains(stringValue)) {
            return null;
        }

        return autoNodeTextExtraction ? stringValue : value.toString();
    }

    @Override
    public Optional<DatatypeMapperFactory<XdmItem>> getDatatypeMapperFactory() {
        return Optional.empty();
    }

    private record XPathPausableSource(
            PausableStaxXmlReader pausableReader, XMLDog xmlDog, Event event, InputStream inputStream)
            implements PausableSource {

        @Override
        public void start() {
            try {
                xmlDog.sniff(event, new InputSource(inputStream), pausableReader);
            } catch (XPathException xPathException) {
                throw new LogicalSourceResolverException("Error starting XML source parsing.", xPathException);
            }
        }

        @Override
        public void pause() {
            pausableReader.pause();
        }

        @Override
        public void resume() {
            try {
                pausableReader.resume();
            } catch (Exception exception) {
                throw new LogicalSourceResolverException("Error reading XML source.", exception);
            }
        }

        @Override
        public boolean isPaused() {
            return pausableReader.isPaused();
        }

        @Override
        public boolean isCompleted() {
            return pausableReader.isCompleted();
        }
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

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, XPathResolver.factory()));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() instanceof XPathReferenceFormulation;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
