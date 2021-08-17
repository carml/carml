package com.taxonic.carml.logicalsourceresolver;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Iterables;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf.Ql;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmItem;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
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

  private static final Function<Object, String> sourceResolver = Object::toString;

  private Processor processor;

  private XPathResolver xpathResolver;

  @BeforeEach
  public void init() {
    processor = new Processor(false);
    XPathCompiler compiler = processor.newXPathCompiler();
    compiler.setCaching(true);
    xpathResolver = XPathResolver.getInstance(processor, compiler, true);
  }

  @Test
  void givenXml_whenSourceFluxApplied_givenCsv_thenReturnFluxOfAllRecords() {
    // Given
    LogicalSourceResolver.SourceFlux<XdmItem> sourceFlux = xpathResolver.getSourceFlux();
    InputStream inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    // When
    Flux<XdmItem> itemFlux = sourceFlux.apply(inputStream, LSOURCE);

    // Then
    StepVerifier.create(itemFlux)
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() throws SaxonApiException {
    // Given
    String expression = "book/author";
    LogicalSourceResolver.ExpressionEvaluationFactory<XdmItem> evaluationFactory =
        xpathResolver.getExpressionEvaluationFactory();
    DocumentBuilder documentBuilder = processor.newDocumentBuilder();
    StringReader reader = new StringReader(BOOK_ONE);
    XdmItem item = documentBuilder.build(new StreamSource(reader));

    ExpressionEvaluation expressionEvaluation = evaluationFactory.apply(item);

    // When
    List<String> values = expressionEvaluation.apply(expression)
        .map(ExpressionEvaluation::extractValues)
        .orElse(List.of());

    // Then
    assertThat(values, hasSize(1));
    assertThat(values, hasItem("Giada De Laurentiis"));
  }

  @Test
  void givenExpression_whenExpressionEvaluationWithoutAutoTextExtractionApplied_thenReturnCorrectValue() {
    LogicalSourceResolver.SourceFlux<XdmItem> sourceFlux = xpathResolver.getSourceFlux();
    InputStream inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);
    Flux<XdmItem> itemFlux = sourceFlux.apply(inputStream, LSOURCE);
    XdmItem item = itemFlux.blockFirst();

    String expression = "./author";
    LogicalSourceResolver.ExpressionEvaluationFactory<XdmItem> evaluationFactory =
        xpathResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation expressionEvaluation = evaluationFactory.apply(item);

    // When
    Optional<Object> evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("Giada De Laurentiis"));

    // redefine XPath resolver to not auto-extract text

    // Given
    boolean autoExtractNodeText = false;
    xpathResolver = XPathResolver.getInstance(autoExtractNodeText);
    sourceFlux = xpathResolver.getSourceFlux();
    inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);
    itemFlux = sourceFlux.apply(inputStream, LSOURCE);
    item = itemFlux.blockFirst();

    evaluationFactory = xpathResolver.getExpressionEvaluationFactory();
    expressionEvaluation = evaluationFactory.apply(item);

    // When
    evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("<author>Giada De Laurentiis</author>"));
  }

  @Test
  void givenExpressionWithNamespace_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
    // Given
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, XPathResolverTest.class.getResourceAsStream("xmlns.rml.ttl"));

    TriplesMap triplesMap = Iterables.getOnlyElement(mapping);
    LogicalSource logicalSource = triplesMap.getLogicalSource();

    LogicalSourceResolver.SourceFlux<XdmItem> sourceFlux = xpathResolver.getSourceFlux();

    Flux<XdmItem> itemFlux = sourceFlux.apply(IOUtils.toInputStream(SOURCE_NS, StandardCharsets.UTF_8), logicalSource);
    XdmItem item = itemFlux.blockFirst();

    String expression = "./ex:author/lower-case(.)";
    LogicalSourceResolver.ExpressionEvaluationFactory<XdmItem> evaluationFactory =
        xpathResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation expressionEvaluation = evaluationFactory.apply(item);

    // When
    Optional<Object> evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("j k. rowling"));
  }
}
