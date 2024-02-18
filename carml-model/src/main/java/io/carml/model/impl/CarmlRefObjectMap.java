package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Join;
import io.carml.model.RefObjectMap;
import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlRefObjectMap extends CarmlResource implements RefObjectMap {

  private TriplesMap parentTriplesMap;

  @Singular
  private Set<Join> joinConditions;

  @RdfProperty(Rr.parentTriplesMap)
  @RdfType(CarmlTriplesMap.class)
  @Override
  public TriplesMap getParentTriplesMap() {
    return parentTriplesMap;
  }

  @RdfProperty(Rr.joinCondition)
  @RdfProperty(value = Carml.multiJoinCondition, deprecated = true)
  @RdfType(CarmlJoin.class)
  @Override
  public Set<Join> getJoinConditions() {
    return joinConditions;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    ImmutableSet.Builder<Resource> builder = ImmutableSet.<Resource>builder();
    if (parentTriplesMap != null) {
      builder.add(parentTriplesMap);
    }
    return builder.addAll(joinConditions)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.RefObjectMap);
    if (parentTriplesMap != null) {
      modelBuilder.add(Rr.parentTriplesMap, parentTriplesMap.getAsResource());
    }
    joinConditions.forEach(jc -> modelBuilder.add(Rr.joinCondition, jc.getAsResource()));
  }
}
