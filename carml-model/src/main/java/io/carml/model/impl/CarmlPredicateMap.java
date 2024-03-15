package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.PredicateMap;
import io.carml.model.Resource;
import io.carml.vocab.Rdf;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CarmlPredicateMap extends CarmlTermMap implements PredicateMap {

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.<Resource>builder()
        .addAll(getReferencedResourcesBase())
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rml.PredicateMap);

    addTriplesBase(modelBuilder);
  }

  @Override
  public PredicateMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter) {
    var predicateMapBuilder = this.toBuilder();
    if (reference != null) {
      adaptReference(referenceExpressionAdapter, predicateMapBuilder::reference);
      return predicateMapBuilder.build();
    } else if (template != null) {
      adaptTemplate(referenceExpressionAdapter, predicateMapBuilder::template);
      return predicateMapBuilder.build();
    } else if (functionValue != null) {
      adaptFunctionValue(referenceExpressionAdapter, predicateMapBuilder::functionValue);
      return predicateMapBuilder.build();
    } else {
      return this;
    }
  }
}
