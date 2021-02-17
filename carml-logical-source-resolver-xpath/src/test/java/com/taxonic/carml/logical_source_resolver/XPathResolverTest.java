package com.taxonic.carml.logical_source_resolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.engine.Item;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.ExpressionEvaluatorFactory;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf.Ql;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmValue;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Before;
import org.junit.Test;

public class XPathResolverTest {

	private static final String BOOK_ONE =
			"<book category=\"cooking\">\r\n" +
			"  <title lang=\"en\">Everyday Italian</title>\r\n" +
			"  <author>Giada De Laurentiis</author>\r\n" +
			"  <year>2005</year>\r\n" +
			"  <price>30.00</price>\r\n" +
			"</book>";

	private static final String SOURCE =
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
			"\r\n" +
			"<bookstore>\r\n" +
			"\r\n" +
			BOOK_ONE +
			"\r\n" +
			"<book category=\"children\">\r\n" +
			"  <title lang=\"en\">Harry Potter</title>\r\n" +
			"  <author>J K. Rowling</author>\r\n" +
			"  <year>2005</year>\r\n" +
			"  <price>29.99</price>\r\n" +
			"</book>\r\n" +
			"\r\n" +
			"</bookstore>";

	private static final LogicalSource LSOURCE =
			new CarmlLogicalSource(SOURCE, "/bookstore/*", Ql.XPath);

	private static final String SOURCE_NS =
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
			"\r\n" +
			"<ex:bookstore xmlns:ex=\"http://www.example.com/books/1.0/\">\r\n" +
			"\r\n" +
			"<ex:book category=\"children\">\r\n" +
			"  <ex:title lang=\"en\">Harry Potter</ex:title>\r\n" +
			"  <ex:author>J K. Rowling</ex:author>\r\n" +
			"  <ex:year>2005</ex:year>\r\n" +
			"  <ex:price>29.99</ex:price>\r\n" +
			"</ex:book>\r\n" +
			"\r\n" +
			"</ex:bookstore>";

	private static final Function<Object, String> nsSourceResolver = s -> SOURCE_NS;

	private static final Function<Object, String> sourceResolver = s -> s.toString();

	private XPathResolver xpathResolver;
	private Supplier<Stream<Item<XdmItem>>> getNodeStream;
	private List<XdmItem> nodes;

	@Before
	public void init() {
		xpathResolver = new XPathResolver();
		getNodeStream = xpathResolver.bindSource(LSOURCE, sourceResolver);
		nodes = getNodeStream.get().map(Item::getItem).collect(Collectors.toList());
	}

	@Test
	public void sourceIterator_givenXmlSourceAndBookNodeSelector_shoulReturnAllNodes() {
		assertThat(nodes.size(), is(2));
		assertThat(nodes.get(0).toString(), is(equalToCompressingWhiteSpace(BOOK_ONE)));
	}

	@Test
	public void expressionEvaluator_givenExpression_shoulReturnCorrectValue() {
		String expression = "./author/lower-case(.)";
		ExpressionEvaluatorFactory<XdmValue> evaluatorFactory =
				xpathResolver.getExpressionEvaluatorFactory();
		EvaluateExpression evaluateExpression = evaluatorFactory.apply(nodes.get(0));
		Object value = evaluateExpression.apply(expression)
			.map(xpathResolver.getCreateSimpleTypedRepresentation()).get();
		assertThat(value, is("giada de laurentiis"));
	}

	@Test
	public void expressionEvaluatorWithoutAutoTextExtraction_givenExpression_shoulReturnCorrectValue() {
		String expression = "./author";
		ExpressionEvaluatorFactory<XdmValue> evaluatorFactory =
				xpathResolver.getExpressionEvaluatorFactory();
		EvaluateExpression evaluateExpression = evaluatorFactory.apply(nodes.get(0));
		Object value = evaluateExpression.apply(expression)
			.map(xpathResolver.getCreateSimpleTypedRepresentation()).get();
		assertThat(value, is("Giada De Laurentiis"));

		// redefine XPath resolver to not auto-extract text
		boolean autoExtractNodeText = false;
		xpathResolver = new XPathResolver(autoExtractNodeText);

		getNodeStream = xpathResolver.bindSource(LSOURCE, sourceResolver);
		evaluatorFactory = xpathResolver.getExpressionEvaluatorFactory();

		nodes = getNodeStream.get().map(Item::getItem).collect(Collectors.toList());
		evaluateExpression = evaluatorFactory.apply(nodes.get(0));
		value = evaluateExpression.apply(expression)
			.map(xpathResolver.getCreateSimpleTypedRepresentation()).get();
		assertThat(value, is("<author>Giada De Laurentiis</author>"));
	}

	@Test
	public void expressionEvaluator_givenExpressionWithNamespace_shoulReturnCorrectValue() {
		Set<TriplesMap> mapping =
				RmlMappingLoader
					.build()
						.load(RDFFormat.TURTLE,
								XPathResolverTest.class.getResourceAsStream("xmlns.rml.ttl"));

		TriplesMap tMap = Iterables.getOnlyElement(mapping);

		LogicalSource lSource = tMap.getLogicalSource();

		xpathResolver = new XPathResolver();
		getNodeStream = xpathResolver.bindSource(lSource, nsSourceResolver);
		nodes = getNodeStream.get().map(Item::getItem).collect(Collectors.toList());

		String expression = "./ex:author/lower-case(.)";
		ExpressionEvaluatorFactory<XdmValue> evaluatorFactory =
				xpathResolver.getExpressionEvaluatorFactory();
		EvaluateExpression evaluateExpression = evaluatorFactory.apply(nodes.get(0));
		Object value = evaluateExpression.apply(expression)
			.map(xpathResolver.getCreateSimpleTypedRepresentation()).get();
		assertThat(value, is("j k. rowling"));
	}
}
