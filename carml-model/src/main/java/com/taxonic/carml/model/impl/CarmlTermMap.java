package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rr;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PACKAGE)
abstract class CarmlTermMap extends CarmlExpressionMap implements TermMap {

  @Setter
  String inverseExpression;

  @Setter
  TermType termType;

  @RdfProperty(Rr.inverseExpression)
  @Override
  public String getInverseExpression() {
    return inverseExpression;
  }

  // TODO https://www.w3.org/TR/r2rml/#dfn-term-type
  @RdfProperty(Rr.termType)
  @Override
  public TermType getTermType() {
    return termType;
  }

  @Override
  void addTriplesBase(ModelBuilder builder) {
    super.addTriplesBase(builder);
    if (inverseExpression != null) {
      builder.add(Rr.inverseExpression, inverseExpression);
    }
    if (termType != null) {
      addTermTypeTriple(builder);
    }
  }

  private void addTermTypeTriple(ModelBuilder builder) {
    switch (termType) {
      case IRI:
        builder.add(Rr.termType, Rdf.Rr.IRI);
        break;
      case LITERAL:
        builder.add(Rr.termType, Rdf.Rr.Literal);
        break;
      case BLANK_NODE:
        builder.add(Rr.termType, Rdf.Rr.BlankNode);
        break;
      default:
        throw new IllegalStateException(String.format("Illegal term type value '%s' encountered.", termType));
    }
  }
}
