package com.taxonic.carml.logicalsourceresolver;

import static com.taxonic.carml.util.LogUtil.exception;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.XmlSource;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.xml.transform.stream.StreamSource;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class XPathResolver implements LogicalSourceResolver<XdmItem> {

  private final Processor xpathProcessor;

  private final XPathCompiler xpathCompiler;

  private final boolean autoNodeTextExtraction;

  public static XPathResolver getInstance() {
    return getInstance(true);
  }

  public static XPathResolver getInstance(boolean autoNodeTextExtraction) {
    Processor processor = new Processor(false);
    XPathCompiler compiler = processor.newXPathCompiler();
    compiler.setCaching(true);
    return getInstance(processor, compiler, autoNodeTextExtraction);
  }

  public static XPathResolver getInstance(Processor xpathProcessor, XPathCompiler xpathCompiler,
      boolean autoNodeTextExtraction) {
    return new XPathResolver(xpathProcessor, xpathCompiler, autoNodeTextExtraction);
  }

  private void setNamespaces(LogicalSource logicalSource) {
    Object source = logicalSource.getSource();
    if (source instanceof XmlSource) {
      ((XmlSource) source).getDeclaredNamespaces()
          .forEach(n -> xpathCompiler.declareNamespace(n.getPrefix(), n.getName()));
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
    DocumentBuilder documentBuilder = xpathProcessor.newDocumentBuilder();
    InputStreamReader reader = new InputStreamReader(inputStream);
    setNamespaces(logicalSource);

    XPathSelector selector;
    try {
      selector = xpathCompiler.compile(logicalSource.getIterator())
          .load();
    } catch (SaxonApiException saxonApiException) {
      throw new LogicalSourceResolverException(String.format("Could not compile %s", logicalSource.getIterator()));
    }

    // Wrap blocking dom building call in mono
    return Mono.fromRunnable(() -> {
      try {
        XdmNode item = documentBuilder.build(new StreamSource(reader));
        selector.setContextItem(item);
      } catch (SaxonApiException saxonApiException) {
        throw new LogicalSourceResolverException(
            String.format("Exception while processing iterator from %s", exception(logicalSource)), saxonApiException);
      }
    })
        .thenReturn(Flux.fromIterable(selector))
        .flatMapMany(f -> f);
  }

  @Override
  public ExpressionEvaluationFactory<XdmItem> getExpressionEvaluationFactory() {
    return entry -> expression -> {
      logEvaluateExpression(expression, LOG);

      try {
        XPathSelector selector = xpathCompiler.compile(expression)
            .load();
        selector.setContextItem(entry);
        XdmValue value = selector.evaluate();

        if (value.size() > 1) {
          List<String> results = new ArrayList<>();
          value.forEach(i -> {
            String stringValue = getItemStringValue(i, value);
            if (stringValue != null) {
              results.add(stringValue);
            }
          });
          return Optional.of(results);
        } else if (value.size() == 0) {
          return Optional.empty();
        }

        XdmItem item = value.itemAt(0);
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
