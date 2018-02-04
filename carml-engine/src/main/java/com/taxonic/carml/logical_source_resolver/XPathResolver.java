package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.XmlSource;
import java.io.StringReader;
import java.util.Optional;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;

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
				XdmItem value = selector.evaluateSingle();
				
				if (value == null) {
					return Optional.empty();
				}
				
				String result = autoNodeTextExtraction ? value.getStringValue() : value.toString();
				return Optional.of(result);
				
			} catch (SaxonApiException e) {
				throw new RuntimeException(String.format(
						"Error applying XPath expression [%s] to entry [%s]", entry, expression), 
						e);
			}
		};
	}
}
