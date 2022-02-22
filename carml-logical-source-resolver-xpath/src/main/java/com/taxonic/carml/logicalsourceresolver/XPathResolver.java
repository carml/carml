package com.taxonic.carml.logicalsourceresolver;

import static com.taxonic.carml.util.LogUtil.exception;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.XmlSource;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Optional;
import javax.xml.xpath.XPathException;
import jlibs.xml.DefaultNamespaceContext;
import jlibs.xml.sax.dog.NodeItem;
import jlibs.xml.sax.dog.XMLDog;
import jlibs.xml.sax.dog.expr.Expression;
import jlibs.xml.sax.dog.expr.InstantEvaluationListener;
import jlibs.xml.sax.dog.sniff.DOMBuilder;
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
import reactor.core.publisher.Flux;

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
    var namespaceContext = (!(xmlDog.nsContext instanceof DefaultNamespaceContext)) ? new DefaultNamespaceContext()
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
  public SourceFlux<XdmItem> getSourceFlux() {
    return this::getXpathResultFlux;
  }

  private Flux<XdmItem> getXpathResultFlux(Object source, LogicalSource logicalSource) {
    if (!(source instanceof InputStream)) {
      throw new LogicalSourceResolverException(
          String.format("No valid input stream provided for logical source %s", exception(logicalSource)));
    }

    return getXpathResultFlux((InputStream) source, logicalSource);
  }

  private Flux<XdmItem> getXpathResultFlux(InputStream inputStream, LogicalSource logicalSource) {
    setNamespaces(logicalSource);
    try {
      xmlDog.addXPath(logicalSource.getIterator());
    } catch (SAXPathException e) {
      e.printStackTrace();
    }
    var event = xmlDog.createEvent();
    event.setXMLBuilder(new DOMBuilder());

    return Flux.create(sink -> {
      event.setListener(new InstantEvaluationListener() {
        private final DocumentBuilder docBuilder = xpathProcessor.newDocumentBuilder();

        @Override
        public void onNodeHit(Expression expression, NodeItem nodeItem) {
          sink.next(docBuilder.wrap(nodeItem.xml));
        }

        @Override
        public void finishedNodeSet(Expression expression) {
          sink.complete();
        }

        @Override
        public void onResult(Expression expression, Object o) {
          sink.next(docBuilder.wrap(o));
        }
      });

      try {
        xmlDog.sniff(event, new InputSource(inputStream), false);
      } catch (XPathException e) {
        sink.error(e);
      }
    });
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
          value.forEach(i -> {
            var stringValue = getItemStringValue(i, value);
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
