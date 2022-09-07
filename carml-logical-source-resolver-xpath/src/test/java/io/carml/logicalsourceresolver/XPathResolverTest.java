package io.carml.logicalsourceresolver;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Iterables;
import io.carml.engine.ExpressionEvaluation;
import io.carml.model.LogicalSource;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlStream;
import io.carml.util.RmlMappingLoader;
import io.carml.vocab.Rdf.Ql;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import javax.xml.transform.stream.StreamSource;
import jlibs.xml.DefaultNamespaceContext;
import jlibs.xml.sax.dog.XMLDog;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmItem;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class XPathResolverTest {

  private static final String BOOK_ONE = //
      "<book category=\"cooking &amp; food\">" //
          + "  <title lang=\"en\">Everyday Italian</title>"//
          + "  <author>Giada De Laurentiis</author>" //
          + "  <year>2005</year>" //
          + "  <price>30.00</price>" //
          + "</book>";

  private static final String SOURCE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" //
      + "<!DOCTYPE bookstore [\n" + "  <!ENTITY reftest \"J K. &amp; Rowling\">\n" + "  <!ELEMENT bookstore (book)*>\n"
      + "  <!ELEMENT book (title|author|year|price)*>\n" + "  <!ATTLIST book\n" + "    category CDATA #REQUIRED>\n"
      + "  <!ELEMENT title (#PCDATA)>\n" + "     <!ATTLIST title\n" + "       lang CDATA #REQUIRED>\n"
      + " <!ELEMENT author (#PCDATA)>\n" + " <!ELEMENT year (#PCDATA)>\n" + " <!ELEMENT price (#PCDATA)>\n" + " ]>" //
      + "<bookstore>" //
      + "  <!-- Data about books -->" //
      + "  <![CDATA[Data about <books>]]>" //
      + "  <?xml-stylesheet type=\"text/xsl\" href=\"style.xsl\"?>" //
      + BOOK_ONE //
      + "<book category=\"children\">" //
      + "  <title lang=\"en\">Harry Potter</title>" //
      + "  <author>&reftest;</author>" //
      + "  <year>2005</year>" //
      + "  <price>29.99</price>" //
      + "</book>" //
      + "</bookstore>";

  private static final LogicalSource LSOURCE = CarmlLogicalSource.builder()
      .source(SOURCE)
      .iterator("/bookstore/*")
      .referenceFormulation(Ql.XPath)
      .build();

  private static final LogicalSource LSOURCE2 = CarmlLogicalSource.builder()
      .source(SOURCE)
      .iterator("/bookstore/*/author")
      .referenceFormulation(Ql.XPath)
      .build();

  private static final LogicalSource LSOURCE_ROOT = CarmlLogicalSource.builder()
      .source(SOURCE)
      .iterator("/bookstore")
      .referenceFormulation(Ql.XPath)
      .build();

  private static final LogicalSource LSOURCE_INVALID = CarmlLogicalSource.builder()
      .source(SOURCE)
      .iterator("/bookstore/\\\\")
      .referenceFormulation(Ql.XPath)
      .build();

  private static final String SOURCE_NS = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" //
      + "<ex:bookstore xmlns:ex=\"http://www.example.com/books/1.0/\">" //
      + "<ex:book category=\"children\">" //
      + "  <ex:title lang=\"en\">Harry Potter</ex:title>" //
      + "  <ex:author>J K. Rowling</ex:author>" //
      + "  <ex:year>2005</ex:year>" //
      + "  <ex:price>29.99</ex:price>" //
      + "</ex:book>" //
      + "</ex:bookstore>";

  private Processor processor;

  private XPathResolver xpathResolver;

  @BeforeEach
  public void init() {
    processor = new Processor(false);
    var compiler = processor.newXPathCompiler();
    compiler.setCaching(true);
    xpathResolver = XPathResolver.getInstance(new XMLDog(new DefaultNamespaceContext()), processor, compiler, true);
  }

  @Test
  void givenXml_whenRecordResolverApplied_thenReturnFluxOfAllRecords() {
    // Given
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(new CarmlStream(), inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE, LSOURCE2));

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(4)
        .verifyComplete();
  }

  @Test
  void givenXmlAndLSourceWithNestedAndRootExpression_whenRecordResolverApplied_thenReturnFluxOfAllRecords() {
    // Given
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(new CarmlStream(), inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE, LSOURCE_ROOT));

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  void givenXmlAndSlowSubscriber_whenRecordResolverApplied_thenReturnFluxOfAllRecords() {
    // Given
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(new CarmlStream(), inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE, LSOURCE2));

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux, 1)
        .expectNextCount(1)
        .thenAwait(Duration.ofMillis(100))
        .thenRequest(1)
        .expectNextCount(1)
        .thenAwait(Duration.ofMillis(100))
        .thenRequest(1)
        .expectNextCount(1)
        .thenAwait(Duration.ofMillis(100))
        .thenRequest(1)
        .expectNextCount(1)
        .thenAwait(Duration.ofMillis(100))
        .verifyComplete();
  }

  @Test
  void givenXmlRecord_whenRecordResolverApplied_thenReturnFluxOfRecord() throws SaxonApiException {
    // Given
    DocumentBuilder docBuilder = processor.newDocumentBuilder();

    var record = docBuilder.build(new StreamSource(new StringReader(BOOK_ONE)));

    var resolvedSource = ResolvedSource.of(new CarmlStream(), record, XdmItem.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE));

    // When
    var records = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(records)
        .expectNextMatches(logicalSourceRecord -> logicalSourceRecord.getRecord()
            .equals(record))
        .verifyComplete();
  }

  @Test
  void givenLogicalSourceWithInvalidXpath_whenRecordResolverApplied_thenThrowException() {
    // Given
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(SOURCE, inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE_INVALID));

    // When
    var recordFlux = recordResolver.apply(resolvedSource);

    // Then
    StepVerifier.create(recordFlux)
        .expectErrorMessage("Error parsing XPath expression: /bookstore/\\\\")
        .verify();
  }

  @Test
  void givenNoLogicalSources_whenRecordResolverApplied_thenThrowException() {
    // Given
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(SOURCE, inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of());

    // When
    var illegalStateException = assertThrows(IllegalStateException.class, () -> recordResolver.apply(resolvedSource));

    // Then
    assertThat(illegalStateException.getMessage(), is("No logical sources registered"));
  }

  @Test
  void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() throws SaxonApiException {
    // Given
    var expression = "book/author";
    var evaluationFactory = xpathResolver.getExpressionEvaluationFactory();

    var documentBuilder = processor.newDocumentBuilder();
    var reader = new StringReader(BOOK_ONE);
    var item = documentBuilder.build(new StreamSource(reader));

    var expressionEvaluation = evaluationFactory.apply(item);

    // When
    var values = expressionEvaluation.apply(expression)
        .map(ExpressionEvaluation::extractValues)
        .orElse(List.of());

    // Then
    assertThat(values, hasSize(1));
    assertThat(values, hasItem("Giada De Laurentiis"));
  }

  @Test
  void givenExpressionWithMultipleResults_whenExpressionEvaluationApplied_thenReturnCorrectValue()
      throws SaxonApiException {
    // Given
    var expression = "tokenize(book/price, '\\.')";
    var evaluationFactory = xpathResolver.getExpressionEvaluationFactory();

    var documentBuilder = processor.newDocumentBuilder();
    var reader = new StringReader(BOOK_ONE);
    var item = documentBuilder.build(new StreamSource(reader));

    var expressionEvaluation = evaluationFactory.apply(item);

    // When
    var values = expressionEvaluation.apply(expression)
        .map(ExpressionEvaluation::extractValues)
        .orElse(List.of());

    // Then
    assertThat(values, hasSize(2));
    assertThat(values, hasItems("30", "00"));
  }

  @Test
  void givenExpression_whenExpressionEvaluationWithoutAutoTextExtractionApplied_thenReturnCorrectValue() {
    var inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    var resolvedSource = ResolvedSource.of(LSOURCE.getSource(), inputStream, InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE));

    var recordFlux = recordResolver.apply(resolvedSource);
    var item = recordFlux.blockFirst();

    var expression = "./author";
    var evaluationFactory = xpathResolver.getExpressionEvaluationFactory();
    var expressionEvaluation = evaluationFactory.apply(item.getRecord());

    // When
    var evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("Giada De Laurentiis"));

    // redefine XPath resolver to not auto-extract text

    // Given
    xpathResolver = XPathResolver.getInstance(false);

    inputStream = IOUtils.toInputStream(SOURCE, StandardCharsets.UTF_8);

    resolvedSource = ResolvedSource.of(LSOURCE.getSource(), inputStream, InputStream.class);
    recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(LSOURCE));

    recordFlux = recordResolver.apply(resolvedSource);
    item = recordFlux.blockFirst();

    evaluationFactory = xpathResolver.getExpressionEvaluationFactory();
    expressionEvaluation = evaluationFactory.apply(item.getRecord());

    // When
    evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("<author>Giada De Laurentiis</author>"));
  }

  @Test
  void givenExpressionWithNamespace_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
    // Given
    var mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, XPathResolverTest.class.getResourceAsStream("xmlns.rml.ttl"));

    var triplesMap = Iterables.getOnlyElement(mapping);
    var logicalSource = triplesMap.getLogicalSource();

    var resolvedSource = ResolvedSource.of(logicalSource.getSource(),
        IOUtils.toInputStream(SOURCE_NS, StandardCharsets.UTF_8), InputStream.class);
    var recordResolver = xpathResolver.getLogicalSourceRecords(Set.of(logicalSource));

    var recordFlux = recordResolver.apply(resolvedSource);
    var item = recordFlux.blockFirst();

    var expression = "./ex:author/lower-case(.)";
    var evaluationFactory = xpathResolver.getExpressionEvaluationFactory();
    var expressionEvaluation = evaluationFactory.apply(item.getRecord());

    // When
    var evaluationResult = expressionEvaluation.apply(expression);

    // Then
    assertThat(evaluationResult.isPresent(), is(true));
    assertThat(evaluationResult.get(), is("j k. rowling"));
  }
}
