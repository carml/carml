package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.DatatypeMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.Resource;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.OldRml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CarmlObjectMap extends CarmlTermMap implements ObjectMap {

  private DatatypeMap datatypeMap;

  private LanguageMap languageMap;

  @RdfProperty(Rml.datatypeMap)
  @RdfProperty(OldRml.datatypeMap)
  @RdfType(CarmlDatatypeMap.class)
  @Override
  public DatatypeMap getDatatypeMap() {
    return datatypeMap;
  }

  @RdfProperty(Rml.languageMap)
  @RdfProperty(OldRml.languageMap)
  @RdfType(CarmlLanguageMap.class)
  @Override
  public LanguageMap getLanguageMap() {
    return languageMap;
  }

  @RdfProperty(Rml.termType)
  @RdfProperty(Rr.termType)
  @Override
  public TermType getTermType() {
    if (termType != null) {
      return termType;
    }

    if (reference != null || languageMap != null || datatypeMap != null) {
      return TermType.LITERAL;
    }

    return TermType.IRI;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    ImmutableSet.Builder<Resource> builder = ImmutableSet.<Resource>builder()
        .addAll(getReferencedResourcesBase());

    if (datatypeMap != null) {
      builder.add(datatypeMap);
    }

    return builder.build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rml.ObjectMap);

    addTriplesBase(modelBuilder);

    if (datatypeMap != null) {
      modelBuilder.add(Rml.datatypeMap, datatypeMap.getAsResource());
    }
    if (languageMap != null) {
      modelBuilder.add(Rml.languageMap, languageMap.getAsResource());
    }
  }

  @Override
  public ObjectMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter) {
    var objectMapBuilder = this.toBuilder();
    if (reference != null) {
      adaptReference(referenceExpressionAdapter, objectMapBuilder::reference);
      return objectMapBuilder.build();
    } else if (template != null) {
      adaptTemplate(referenceExpressionAdapter, objectMapBuilder::template);
      return objectMapBuilder.build();
    } else if (functionValue != null) {
      adaptFunctionValue(referenceExpressionAdapter, objectMapBuilder::functionValue);
      return objectMapBuilder.build();
    } else {
      return this;
    }
  }
}
