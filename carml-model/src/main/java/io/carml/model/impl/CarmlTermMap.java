package io.carml.model.impl;

import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
abstract class CarmlTermMap extends CarmlExpressionMap implements TermMap {

  String inverseExpression;

  TermType termType;

  @RdfProperty(Rr.inverseExpression)
  @Override
  public String getInverseExpression() {
    return inverseExpression;
  }

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
