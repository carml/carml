package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.XmlSource;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;

public class XPathResolver implements LogicalSourceResolver<XdmItem> {
	
	private Processor xpathProcessor;
	private XPathCompiler xpath;
	private boolean autoNodeTextExtraction;
	
	public XPathResolver() {
		this(true);
	}
	
	public XPathResolver(boolean autoNodeTextExtraction) {
		this.autoNodeTextExtraction = autoNodeTextExtraction;
		this.xpathProcessor = new Processor(false);
		this.xpath = xpathProcessor.newXPathCompiler();
		this.xpath.setCaching(true);
	}
	
	public boolean autoExtractsNodeText() {
		return autoNodeTextExtraction;
	}
	
	private void setNamespaces(LogicalSource logicalSource) {
		Object source = logicalSource.getSource();
		if (source instanceof XmlSource) {
			((XmlSource)source).getDeclaredNamespaces()
			.forEach(n -> xpath.declareNamespace(n.getPrefix(), n.getName()));
		}
	}
	
	@Override
	public SourceIterator<XdmItem> getSourceIterator() {
		return this::getIterableXpathResult;
	}
	
	private Iterable<XdmItem> getIterableXpathResult(String source, LogicalSource logicalSource) {
		DocumentBuilder documentBuilder = xpathProcessor.newDocumentBuilder();
		StringReader reader = new StringReader(source);
		setNamespaces(logicalSource);
		
		try {
			XdmNode item = documentBuilder.build(new StreamSource(reader));
			XPathSelector selector = xpath.compile(logicalSource.getIterator()).load();
			selector.setContextItem(item);
			return selector;
		} catch (SaxonApiException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ExpressionEvaluatorFactory<XdmItem> getExpressionEvaluatorFactory() {
		return entry -> expression -> {
			try {
				XPathSelector selector = xpath.compile(expression).load();
				selector.setContextItem(entry);
				XdmValue value = selector.evaluate();
				
				if (value.size() > 1) {
					List<String> results = new ArrayList<>();
					value.forEach(i -> {
							String sValue = getItemStringValue(i, value);
							if (sValue != null) {
								results.add(sValue);
							}
						}
					);
					return Optional.of(results);
				} else if (value.size() == 0) {
					return Optional.empty();
				}

				XdmItem item = value.itemAt(0);
				return Optional.ofNullable(getItemStringValue(item, value));
				
				
			} catch (SaxonApiException e) {
				throw new RuntimeException(String.format(
						"Error applying XPath expression [%s] to entry [%s]", entry, expression), 
						e);
			}
		};
	}

	@Override
	public GetIterableFromContext<XdmItem> createGetIterableFromContext(String iterator) {
		throw new UnsupportedOperationException("not implemented - in order to use nested mappings with xml/xpath, this method must be implemented");
	}

	@Override
	public CreateContextEvaluate getCreateContextEvaluate() {
		throw new UnsupportedOperationException("not implemented - in order to use nested mappings with xml/xpath, this method must be implemented");
	}

	private String getItemStringValue(XdmItem item, XdmValue value) {
		if (item.getStringValue().length() == 0) {
			return null;
		}

		return autoNodeTextExtraction ? item.getStringValue() : value.toString();
	}
}
