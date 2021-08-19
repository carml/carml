package com.taxonic.carml.engine;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("TODO")
class OldTermGeneratorCreatorTest {

  @Test
  void test() {
    assertThat(true, is(true));
  }

  // @Test
  // public void getGenerator_withReferenceAndTemplate_throwsRuntimeException() throws Exception {
  //
  // TermGeneratorCreator tgc = new TermGeneratorCreator(null, "foo", null, TemplateParser.build(),
  // null);
  //
  // RuntimeException exception = assertThrows(RuntimeException.class, () -> tgc.getObjectGenerator(
  // new CarmlObjectMap("foo.bar", null, "foo{foo.bar}", TermType.LITERAL, null, null, null, null)));
  // assertNotNull(exception);
  // assertTrue(exception.getMessage()
  // .startsWith("2 value generators were created for term map"));
  // }
  //
  // @Test
  // public void IriTermGenerator_withRelativeIRI_usesBaseIRI() throws Exception {
  //
  // ValueFactory f = SimpleValueFactory.getInstance();
  //
  // String baseIri = "http://base.iri";
  // TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(),
  // null);
  //
  // String ref = "ref";
  // TermGenerator<Value> generator =
  // tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null,
  // null));
  //
  // String relativeIriPart = "/relativeIriPortion";
  // ExpressionEvaluation evaluator = Mockito.mock(ExpressionEvaluation.class);
  // when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));
  // List<Value> result = generator.apply(evaluator);
  //
  // assertTrue(!result.isEmpty());
  // assertTrue(result.get(0) instanceof IRI);
  // assertThat(result.get(0), is(f.createIRI(baseIri + relativeIriPart)));
  // }
  //
  // @Test
  // public void IriTermGenerator_withAbsoluteIRI_usesBaseIRI() throws Exception {
  //
  // ValueFactory f = SimpleValueFactory.getInstance();
  //
  // String baseIri = "http://base.iri";
  // TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(),
  // null);
  //
  // String ref = "ref";
  // TermGenerator<Value> generator =
  // tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null,
  // null));
  //
  // String absoluteIri = "http://foo.bar/absoluteIri";
  // ExpressionEvaluation evaluator = Mockito.mock(ExpressionEvaluation.class);
  // when(evaluator.apply(ref)).thenReturn(Optional.of(absoluteIri));
  // List<Value> result = generator.apply(evaluator);
  //
  // assertTrue(!result.isEmpty());
  // assertTrue(result.get(0) instanceof IRI);
  // assertThat(result.get(0), is(f.createIRI(absoluteIri)));
  // }
  //
  // @Test
  // public void IriTermGenerator_withFaultyIRI_throwsException() throws Exception {
  //
  // ValueFactory f = SimpleValueFactory.getInstance();
  //
  // String baseIri = "?";
  // TermGeneratorCreator tgc = new TermGeneratorCreator(f, baseIri, null, TemplateParser.build(),
  // null);
  //
  // String ref = "ref";
  // TermGenerator<Value> generator =
  // tgc.getObjectGenerator(new CarmlObjectMap(ref, null, null, TermType.IRI, null, null, null,
  // null));
  //
  // String relativeIriPart = "/relativeIriPortion";
  // ExpressionEvaluation evaluator = Mockito.mock(ExpressionEvaluation.class);
  // when(evaluator.apply(ref)).thenReturn(Optional.of(relativeIriPart));
  //
  // RuntimeException exception = null;
  // try {
  // generator.apply(evaluator);
  // // assertTrue("This code should be unreachable", false);
  // } catch (RuntimeException e) {
  // exception = e;
  // }
  //
  // assertNotNull(exception);
  // assertTrue(exception.getMessage()
  // .startsWith("Could not generate a valid iri"));
  // }

}
