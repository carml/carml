package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.engine.Item;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.XmlSource;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.event.ReceiverOption;
import net.sf.saxon.event.TreeReceiver;
import net.sf.saxon.expr.parser.Loc;
import net.sf.saxon.om.EmptyAttributeMap;
import net.sf.saxon.om.NamespaceMap;
import net.sf.saxon.om.NoNamespaceName;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.serialize.SerializationProperties;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.Untyped;

import static com.taxonic.carml.logical_source_resolver.util.ContextUtils.createContextMap;

public class XPathResolver implements LogicalSourceResolver<XdmItem> {

	private final boolean autoNodeTextExtraction;
	private final Processor xpathProcessor;
	private final Configuration configuration;
	private final XPathCompiler xpath;

	public XPathResolver() {
		this(true);
	}
	
	public XPathResolver(boolean autoNodeTextExtraction) {
		this.autoNodeTextExtraction = autoNodeTextExtraction;
		xpathProcessor = new Processor(false);
		configuration = xpathProcessor.getUnderlyingConfiguration();
		xpath = xpathProcessor.newXPathCompiler();
		xpath.setCaching(true);
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
	public SourceStream<XdmItem> getSourceStream() {
		return this::getXpathResultAsStream;
	}
	
	private Stream<Item<XdmItem>> getXpathResultAsStream(String stringSource, LogicalSource logicalSource) {
		setNamespaces(logicalSource);
		StreamSource source = new StreamSource(new StringReader(stringSource));
		DocumentBuilder documentBuilder = xpathProcessor.newDocumentBuilder();

		try {
			XdmNode item = documentBuilder.build(source);
			XPathSelector selector = xpath.compile(logicalSource.getIterator()).load();
			selector.setContextItem(item);
			ExpressionEvaluatorFactory<XdmValue> evaluatorFactory = getExpressionEvaluatorFactory();
			return selector.stream()
				.map(o -> new Item<>(o, evaluatorFactory.apply(o)));
		} catch (SaxonApiException e) {
			throw new RuntimeException(e);
		}
	}

	ExpressionEvaluatorFactory<XdmValue> getExpressionEvaluatorFactory() {
		return entry -> expression -> {

			XPathSelector selector;
			try {
				selector = xpath.compile(expression).load();
			}
			catch (SaxonApiException e) {
				throw new RuntimeException(String.format(
					"Error applying XPath expression [%s] to entry [%s]", expression, entry),
					e);
			}

			if (entry.size() == 1) {
				try {
					selector.setContextItem(entry.itemAt(0));
					XdmValue value = selector.evaluate();
					switch (value.size()) {
						case 0:
							return Optional.empty();
						case 1:
							return Optional.<Object>of(value.itemAt(0))
								.filter(i -> getItemStringValue((XdmItem) i, value) != null);
						default:
							return createResultFromSequence(value);
					}
				}
				catch (SaxonApiException e) {
					throw new RuntimeException(String.format(
						"Error applying XPath expression [%s] to entry [%s]", expression, entry),
						e);
				}
			}

			List<? extends XdmItem> items = entry.stream().flatMap(i -> {
				try {
					selector.setContextItem(i);
					XdmValue value = selector.evaluate();
					return value.stream()
						.filter(j -> getItemStringValue(j, value) != null);
				}
				catch (SaxonApiException e) {
					throw new RuntimeException(String.format(
						"Error applying XPath expression [%s] to item [%s] of entry [%s]", expression, i, entry),
						e);
				}
			})
			.collect(Collectors.toList());

			return items.isEmpty()
				? Optional.empty()
				: Optional.of(new XdmValue(items));
		};
	}

	private Optional<Object> createResultFromSequence(XdmValue sequence) {
		List<? extends XdmItem> items = sequence.stream()
			.filter(j -> getItemStringValue(j, sequence) != null)
			.collect(Collectors.toList());
		return items.isEmpty()
			? Optional.empty()
			: Optional.of(new XdmValue(items));
	}

	@Override
	public GetStreamFromContext<XdmItem> createGetStreamFromContext(String iterator) {

		ExpressionEvaluatorFactory<XdmValue> evaluatorFactory = getExpressionEvaluatorFactory();

		return e -> e.apply(iterator)
			.map(XdmValue.class::cast)
			.map(v -> v.stream().map(o -> new Item<XdmItem>(o, evaluatorFactory.apply(o))))
			.orElse(Stream.empty());
	}

	@Override
	public CreateContextEvaluate getCreateContextEvaluate() {

		ExpressionEvaluatorFactory<XdmValue> f = getExpressionEvaluatorFactory();
		return (entries, evaluate) -> {

			Map<String, Object> contextMap = createContextMap(entries, evaluate);

			XdmDestination destination = new XdmDestination();
			Receiver xdmReceiver = destination.getReceiver(configuration.makePipelineConfiguration(), new SerializationProperties());
			TreeReceiver receiver = new TreeReceiver(xdmReceiver);

			try {
				receiver.open();
				receiver.startDocument(ReceiverOption.NONE);
				for (Map.Entry<String, Object> entry : contextMap.entrySet()) {
					String key = entry.getKey();
					XdmValue value = (XdmValue) entry.getValue();

					// start "as"-element
					receiver.startElement(
						new NoNamespaceName(key),
						Untyped.INSTANCE,
						EmptyAttributeMap.getInstance(),
						new NamespaceMap(Collections.emptyList()),
						Loc.NONE,
						0
					);

					// append evaluated context item(s)
					// TODO probably more configuration options re: wrapping / not wrapping
					//      of items, using a single item as the "as" element, etc.
					for (XdmItem item : value) {
						receiver.append(item.getUnderlyingValue());
					}
					receiver.endElement();
				}
				receiver.endDocument();
				receiver.close();
			}
			catch (XPathException e) {
				throw new RuntimeException(e);
			}

			XdmNode context = destination.getXdmNode();
			return f.apply(context);
		};
	}

	@Override
	public CreateSimpleTypedRepresentation getCreateSimpleTypedRepresentation() {
		return o -> {
			XdmValue value = (XdmValue) o;
			switch (value.size()) {
				case 0:
					// NOTE: since our EvaluateExpression returns Optional.empty() in case of 0 items,
					// this case should never occur.
					return Collections.emptyList();
				case 1:
					XdmItem item = value.itemAt(0);
					return getItemStringValue(item, value);
				default:
					return value.stream()
						.map(i -> getItemStringValue(i, value))
						.filter(Objects::nonNull)
						.collect(Collectors.toList());
			}
		};
	}

	private String getItemStringValue(XdmItem item, XdmValue value) {
		if (item.getStringValue().length() == 0) {
			return null;
		}

		return autoNodeTextExtraction ? item.getStringValue() : value.toString();
	}
}
