package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.XmlSource;
import io.carml.vocab.Rdf.Ql;
import io.carml.vocab.Rdf.Rml;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
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
import org.eclipse.rdf4j.model.IRI;
import org.jaxen.saxpath.SAXPathException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class XPathResolver implements LogicalSourceResolver<XdmItem> {

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
        var source = logicalSource.getSource();
        if (source instanceof XmlSource xmlSource) {
            xmlSource.getDeclaredNamespaces().forEach(n -> {
                nsContext.declarePrefix(n.getPrefix(), n.getName());
                xpathCompiler.declareNamespace(n.getPrefix(), n.getName());
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

        if (resolvedSource == null || resolvedSource.getResolved().isEmpty()) {
            throw new LogicalSourceResolverException(
                    String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
        }

        var resolved = resolvedSource.getResolved().get();

        if (resolved instanceof InputStream resolvedInputStream) {
            return getXpathResultFlux(resolvedInputStream, logicalSources);
        } else if (resolved instanceof XdmItem resolvedXdmItem) {
            return getXpathResultFlux(resolvedXdmItem, logicalSources);
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(
            InputStream inputStream, Set<LogicalSource> logicalSources) {
        var outstandingRequests = new AtomicLong();
        var pausableReader = new PausableStaxXmlReader();

        return Flux.create(
                sink -> xpathPathFlux(sink, logicalSources, inputStream, outstandingRequests, pausableReader));
    }

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(XdmItem xdmItem, Set<LogicalSource> logicalSources) {
        return Flux.fromIterable(logicalSources)
                .flatMap(logicalSource -> getXpathResultFluxForLogicalSource(xdmItem, logicalSource));
    }

    private void xpathPathFlux(
            FluxSink<LogicalSourceRecord<XdmItem>> sink,
            Set<LogicalSource> logicalSources,
            InputStream inputStream,
            AtomicLong outstandingRequests,
            PausableStaxXmlReader pausableReader) {
        sink.onRequest(requested -> {
            var outstanding = outstandingRequests.addAndGet(requested);
            if (pausableReader.isPaused() && outstanding >= 0L) {
                if (!pausableReader.isCompleted()) {
                    try {
                        pausableReader.resume();
                    } catch (SAXException | XMLStreamException xmlReadingException) {
                        sink.error(
                                new LogicalSourceResolverException("Error reading XML source.", xmlReadingException));
                    }
                }
            } else {
                checkReaderToPause(outstanding, pausableReader);
            }
        });
        sink.onDispose(() -> cleanup(inputStream));

        Map<String, LogicalSource> logicalSourceByExpression = new HashMap<>();
        XMLDog xmlDog = prepareXmlDog(sink, logicalSources, logicalSourceByExpression);
        var event = xmlDog.createEvent();
        bridgeAndListen(logicalSourceByExpression, event, sink, outstandingRequests, pausableReader);

        try {
            xmlDog.sniff(event, new InputSource(inputStream), pausableReader);
        } catch (XPathException xpathException) {
            sink.error(new LogicalSourceResolverException("Error executing XPath expression.", xpathException));
        }
    }

    private XMLDog prepareXmlDog(
            FluxSink<LogicalSourceRecord<XdmItem>> sink,
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
                    sink.error(new LogicalSourceResolverException(
                            String.format("Error parsing XPath expression: %s", logicalSource.getIterator()),
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
            FluxSink<LogicalSourceRecord<XdmItem>> sink,
            AtomicLong outstandingRequests,
            PausableStaxXmlReader pausableReader) {

        var expressionCompletion = logicalSourceByExpression.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> false));

        event.setXMLBuilder(new DOMBuilder());
        event.setListener(new InstantEvaluationListener() {
            private final DocumentBuilder docBuilder = xpathProcessor.newDocumentBuilder();

            @Override
            public void onNodeHit(Expression expression, NodeItem nodeItem) {
                var logicalSource = logicalSourceByExpression.get(expression.getXPath());
                sink.next(LogicalSourceRecord.of(logicalSource, docBuilder.wrap(nodeItem.xml)));
                var outstanding = outstandingRequests.decrementAndGet();
                checkReaderToPause(outstanding, pausableReader);
            }

            @Override
            public void finishedNodeSet(Expression expression) {
                expressionCompletion.put(expression.getXPath(), true);
                if (expressionCompletion.values().stream().allMatch(Boolean::valueOf)) {
                    sink.complete();
                }
            }

            @Override
            public void onResult(Expression expression, Object object) {
                var logicalSource = logicalSourceByExpression.get(expression.getXPath());
                sink.next(LogicalSourceRecord.of(logicalSource, docBuilder.wrap(object)));
                var outstanding = outstandingRequests.decrementAndGet();
                checkReaderToPause(outstanding, pausableReader);
            }
        });
    }

    private void checkReaderToPause(long outstanding, PausableStaxXmlReader pausableReader) {
        if (!pausableReader.isPaused() && outstanding < 0L) {
            pausableReader.pause();
        }
    }

    private void cleanup(InputStream inputStream) {
        try {
            inputStream.close();
        } catch (IOException ioException) {
            throw new LogicalSourceResolverException("Error closing input stream.", ioException);
        }
    }

    private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFluxForLogicalSource(
            XdmItem xdmItem, LogicalSource logicalSource) {
        try {
            var selector = xpathCompiler.compile(logicalSource.getIterator()).load();
            selector.setContextItem(xdmItem);
            var value = selector.evaluate();

            if (value.isEmpty()) {
                return Flux.empty();
            }

            return Flux.fromIterable(value).map(item -> LogicalSourceRecord.of(logicalSource, item));
        } catch (SaxonApiException e) {
            throw new LogicalSourceResolverException(
                    String.format(
                            "Error applying XPath expression [%s] to entry [%s]", logicalSource.getIterator(), xdmItem),
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
                    var results = new ArrayList<>();
                    value.forEach(item -> {
                        var stringValue = getItemStringValue(item, value);
                        if (stringValue != null) {
                            results.add(stringValue);
                        }
                    });
                    return Optional.of(results);
                } else if (value.isEmpty()) {
                    return Optional.empty();
                }

                var item = value.itemAt(0);
                return Optional.ofNullable(getItemStringValue(item, value));
            } catch (SaxonApiException e) {
                throw new LogicalSourceResolverException(
                        String.format("Error applying XPath expression [%s] to entry [%s]", expression, entry), e);
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

    @ToString
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Matcher implements MatchingLogicalSourceResolverFactory {
        private static final Set<IRI> MATCHING_REF_FORMULATIONS = Set.of(Rml.XPath, Ql.XPath);

        private List<IRI> matchingReferenceFormulations;

        public static Matcher getInstance() {
            return getInstance(Set.of());
        }

        public static Matcher getInstance(Set<IRI> customMatchingReferenceFormulations) {
            return new Matcher(
                    Stream.concat(customMatchingReferenceFormulations.stream(), MATCHING_REF_FORMULATIONS.stream())
                            .distinct()
                            .toList());
        }

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
            return logicalSource.getReferenceFormulation() != null
                    && matchingReferenceFormulations.contains(logicalSource.getReferenceFormulation());
        }

        @Override
        public String getResolverName() {
            return "XPathResolver";
        }
    }
}
