package com.taxonic.carml.logical_source_resolver;


import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf.Ql;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmItem;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class XPathResolverTest {

  private static final String BOOK_ONE = "<book category=\"cooking\">\r\n"
      + "  <title lang=\"en\">Everyday Italian</title>\r\n" + "  <author>Giada De Laurentiis</author>\r\n"
      + "  <year>2005</year>\r\n" + "  <price>30.00</price>\r\n" + "</book>";

  private static final String SOURCE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" + "\r\n" + "<bookstore>\r\n"
      + "\r\n" + BOOK_ONE + "\r\n" + "<book category=\"children\">\r\n"
      + "  <title lang=\"en\">Harry Potter</title>\r\n" + "  <author>J K. Rowling</author>\r\n"
      + "  <year>2005</year>\r\n" + "  <price>29.99</price>\r\n" + "</book>\r\n" + "\r\n" + "</bookstore>";

  private static final LogicalSource LSOURCE = new CarmlLogicalSource(SOURCE, "/bookstore/*", Ql.XPath);

  private static final String SOURCE_NS = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" + "\r\n"
      + "<ex:bookstore xmlns:ex=\"http://www.example.com/books/1.0/\">\r\n" + "\r\n"
      + "<ex:book category=\"children\">\r\n" + "  <ex:title lang=\"en\">Harry Potter</ex:title>\r\n"
      + "  <ex:author>J K. Rowling</ex:author>\r\n" + "  <ex:year>2005</ex:year>\r\n"
      + "  <ex:price>29.99</ex:price>\r\n" + "</ex:book>\r\n" + "\r\n" + "</ex:bookstore>";

  private static final Function<Object, String> nsSourceResolver = s -> SOURCE_NS;

  private static final Function<Object, String> sourceResolver = s -> s.toString();

  private Processor processor;

  private XPathResolver xPathResolver;

  @BeforeEach
  public void init() {
    processor = new Processor(false);
    XPathCompiler compiler = processor.newXPathCompiler();
    compiler.setCaching(true);
    xPathResolver = XPathResolver.getInstance(processor, compiler, true);
  }

  @Test
  void given_when_then() {
    // Given
    LogicalSourceResolver.SourceFlux<XdmItem> sourceFlux = xPathResolver.getSourceFlux();

    // When
    InputStream inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    // Then
    StepVerifier.create(sourceFlux.apply(inputStream, LSOURCE))
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  public void givenExpression_whenExpressionEvaluation_thenReturnCorrectValue() throws SaxonApiException {
    String expression = "book/author";
    LogicalSourceResolver.ExpressionEvaluationFactory<XdmItem> evaluationFactory =
        xPathResolver.getExpressionEvaluationFactory();
    DocumentBuilder documentBuilder = processor.newDocumentBuilder();
    StringReader reader = new StringReader(BOOK_ONE);
    XdmItem item = documentBuilder.build(new StreamSource(reader));

    ExpressionEvaluation expressionEvaluation = evaluationFactory.apply(item);
    List<String> values = ExpressionEvaluation.extractValues(expressionEvaluation.apply(expression));
    System.out.println(values);
  }
  //
  // @Test
  // public void
  // expressionEvaluatorWithoutAutoTextExtraction_givenExpression_shoulReturnCorrectValue() {
  // String expression = "./author";
  // ExpressionEvaluatorFactory<XdmItem> evaluatorFactory =
  // xpathResolver.getExpressionEvaluatorFactory();
  // ExpressionEvaluation expressionEvaluation = evaluatorFactory.apply(nodes.get(0));
  // assertThat(expressionEvaluation.apply(expression)
  // .get(), is("Giada De Laurentiis"));
  //
  // // redefine XPath resolver to not auto-extract text
  // boolean autoExtractNodeText = false;
  // xpathResolver = new XPathResolver(autoExtractNodeText);
  //
  // nodeIterator = xpathResolver.bindSource(LSOURCE, sourceResolver)
  // .get();
  // evaluatorFactory = xpathResolver.getExpressionEvaluatorFactory();
  //
  // nodes = Lists.newArrayList(nodeIterator);
  // expressionEvaluation = evaluatorFactory.apply(nodes.get(0));
  // assertThat(expressionEvaluation.apply(expression)
  // .get(), is("<author>Giada De Laurentiis</author>"));
  // }
  //
  // @Test
  // public void expressionEvaluator_givenExpressionWithNamespace_shoulReturnCorrectValue() {
  // Set<TriplesMap> mapping = RmlMappingLoader.build()
  // .load(RDFFormat.TURTLE, XPathResolverTest.class.getResourceAsStream("xmlns.rml.ttl"));
  //
  // TriplesMap tMap = Iterables.getOnlyElement(mapping);
  //
  // LogicalSource lSource = tMap.getLogicalSource();
  //
  // xpathResolver = new XPathResolver();
  // nodeIterator = xpathResolver.bindSource(lSource, nsSourceResolver)
  // .get();
  // nodes = Lists.newArrayList(nodeIterator);
  //
  // String expression = "./ex:author/lower-case(.)";
  // ExpressionEvaluatorFactory<XdmItem> evaluatorFactory =
  // xpathResolver.getExpressionEvaluatorFactory();
  // ExpressionEvaluation expressionEvaluation = evaluatorFactory.apply(nodes.get(0));
  // assertThat(expressionEvaluation.apply(expression)
  // .get(), is("j k. rowling"));
  // }
}
