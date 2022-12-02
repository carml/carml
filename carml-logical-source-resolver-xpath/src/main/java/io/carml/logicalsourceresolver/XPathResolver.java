package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.LogicalSource;
import io.carml.model.XmlSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import lombok.extern.slf4j.Slf4j;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmValue;
import org.jaxen.saxpath.SAXPathException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class XPathResolver implements LogicalSourceResolver<XdmItem> {

  private final DefaultNamespaceContext nsContext;

  private final XMLDog xmlDog;

  private final Processor xpathProcessor;

  private final XPathCompiler xpathCompiler;

  private final boolean autoNodeTextExtraction;

  public static XPathResolver getInstance() {
    return getInstance(true);
  }

  public static XPathResolver getInstance(boolean autoNodeTextExtraction) {
    var processor = new Processor(false);
    var compiler = processor.newXPathCompiler();
    compiler.setCaching(true);
    var xmlDog = new XMLDog(new DefaultNamespaceContext());
    return getInstance(xmlDog, processor, compiler, autoNodeTextExtraction);
  }

  public static XPathResolver getInstance(XMLDog xmlDog, Processor xpathProcessor, XPathCompiler xpathCompiler,
      boolean autoNodeTextExtraction) {
    var namespaceContext = !(xmlDog.nsContext instanceof DefaultNamespaceContext) ? new DefaultNamespaceContext()
        : (DefaultNamespaceContext) xmlDog.nsContext;

    return new XPathResolver(namespaceContext, xmlDog, xpathProcessor, xpathCompiler, autoNodeTextExtraction);
  }

  private void setNamespaces(LogicalSource logicalSource) {
    var source = logicalSource.getSource();
    if (source instanceof XmlSource) {
      ((XmlSource) source).getDeclaredNamespaces()
          .forEach(n -> {
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

  private Flux<LogicalSourceRecord<XdmItem>> getLogicalSourceRecordFlux(ResolvedSource<?> resolvedSource,
      Set<LogicalSource> logicalSources) {
    if (logicalSources.isEmpty()) {
      throw new IllegalStateException("No logical sources registered");
    }

    if (resolvedSource == null || resolvedSource.getResolved()
        .isEmpty()) {
      throw new LogicalSourceResolverException(
          String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
    }

    var resolved = resolvedSource.getResolved()
        .get();

    if (resolved instanceof InputStream) {
      return getXpathResultFlux((InputStream) resolvedSource.getResolved()
          .get(), logicalSources);
    } else if (resolved instanceof XdmItem) {
      return getXpathResultFlux((XdmItem) resolved, logicalSources);
    } else {
      throw new LogicalSourceResolverException(
          String.format("Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
    }
  }

  private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(InputStream inputStream,
      Set<LogicalSource> logicalSources) {
    var outstandingRequests = new AtomicLong();
    var pausableReader = new PausableStaxXmlReader();

    return Flux.create(sink -> xpathPathFlux(sink, logicalSources, inputStream, outstandingRequests, pausableReader));
  }

  private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFlux(XdmItem xdmItem, Set<LogicalSource> logicalSources) {
    return Flux.fromIterable(logicalSources)
        .flatMap(logicalSource -> getXpathResultFluxForLogicalSource(xdmItem, logicalSource));
  }

  private void xpathPathFlux(FluxSink<LogicalSourceRecord<XdmItem>> sink, Set<LogicalSource> logicalSources,
      InputStream inputStream, AtomicLong outstandingRequests, PausableStaxXmlReader pausableReader) {
    sink.onRequest(requested -> {
      var outstanding = outstandingRequests.addAndGet(requested);
      if (pausableReader.isPaused() && outstanding >= 0L) {
        if (!pausableReader.isCompleted()) {
          try {
            pausableReader.resume();
          } catch (SAXException | XMLStreamException xmlReadingException) {
            sink.error(new LogicalSourceResolverException("Error reading XML source.", xmlReadingException));
          }
        }
      } else {
        checkReaderToPause(outstanding, pausableReader);
      }
    });
    sink.onDispose(() -> cleanup(inputStream));

    Map<Expression, LogicalSource> logicalSourceByExpression = new HashMap<>();
    logicalSources.forEach(logicalSource -> {
      setNamespaces(logicalSource);
      try {
        logicalSourceByExpression.put(xmlDog.addXPath(logicalSource.getIterator()), logicalSource);
      } catch (SAXPathException saxPathException) {
        sink.error(new LogicalSourceResolverException(
            String.format("Error parsing XPath expression: %s", logicalSource.getIterator()), saxPathException));
      }
    });

    var event = xmlDog.createEvent();
    bridgeAndListen(logicalSourceByExpression, event, sink, outstandingRequests, pausableReader);

    try {
      xmlDog.sniff(event, new InputSource(inputStream), pausableReader);
    } catch (XPathException xpathException) {
      sink.error(new LogicalSourceResolverException("Error executing XPath expression.", xpathException));
    }
  }

  private void bridgeAndListen(Map<Expression, LogicalSource> logicalSourceByExpression, Event event,
      FluxSink<LogicalSourceRecord<XdmItem>> sink, AtomicLong outstandingRequests,
      PausableStaxXmlReader pausableReader) {

    var expressionCompletion = logicalSourceByExpression.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> false));

    event.setXMLBuilder(new DOMBuilder());
    event.setListener(new InstantEvaluationListener() {
      private final DocumentBuilder docBuilder = xpathProcessor.newDocumentBuilder();

      @Override
      public void onNodeHit(Expression expression, NodeItem nodeItem) {
        var logicalSource = logicalSourceByExpression.get(expression);
        sink.next(LogicalSourceRecord.of(logicalSource, docBuilder.wrap(nodeItem.xml)));
        var outstanding = outstandingRequests.decrementAndGet();
        checkReaderToPause(outstanding, pausableReader);
      }

      @Override
      public void finishedNodeSet(Expression expression) {
        expressionCompletion.put(expression, true);
        if (expressionCompletion.values()
            .stream()
            .allMatch(Boolean::valueOf)) {
          sink.complete();
        }
      }

      @Override
      public void onResult(Expression expression, Object o) {
        var logicalSource = logicalSourceByExpression.get(expression);
        sink.next(LogicalSourceRecord.of(logicalSource, docBuilder.wrap(o)));
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

  private Flux<LogicalSourceRecord<XdmItem>> getXpathResultFluxForLogicalSource(XdmItem xdmItem,
      LogicalSource logicalSource) {
    try {
      var selector = xpathCompiler.compile(logicalSource.getIterator())
          .load();
      selector.setContextItem(xdmItem);
      var value = selector.evaluate();

      if (value.isEmpty()) {
        return Flux.empty();
      }

      return Flux.fromIterable(value)
          .map(item -> LogicalSourceRecord.of(logicalSource, item));
    } catch (SaxonApiException e) {
      throw new LogicalSourceResolverException(
          String.format("Error applying XPath expression [%s] to entry [%s]", logicalSource.getIterator(), xdmItem), e);
    }
  }

  @Override
  public ExpressionEvaluationFactory<XdmItem> getExpressionEvaluationFactory() {
    return entry -> expression -> {
      logEvaluateExpression(expression, LOG);

      try {
        var selector = xpathCompiler.compile(expression)
            .load();
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
        } else if (value.size() == 0) {
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
    if (item.getStringValue()
        .length() == 0) {
      return null;
    }

    return autoNodeTextExtraction ? item.getStringValue() : value.toString();
  }
}
