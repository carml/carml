package io.carml.model.impl;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import io.carml.model.Join;
import io.carml.model.RefObjectMap;
import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
public class CarmlRefObjectMap extends CarmlResource implements RefObjectMap {

  private TriplesMap parentTriplesMap;

  @Singular
  private Set<Join> joinConditions;

  @RdfProperty(Rml.parentTriplesMap)
  @RdfProperty(Rr.parentTriplesMap)
  @RdfType(CarmlTriplesMap.class)
  @Override
  public TriplesMap getParentTriplesMap() {
    return parentTriplesMap;
  }

  @RdfProperty(Rml.joinCondition)
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
        .add(RDF.TYPE, Rdf.Rml.RefObjectMap);
    if (parentTriplesMap != null) {
      modelBuilder.add(Rdf.Rml.parentTriplesMap, parentTriplesMap.getAsResource());
    }
    joinConditions.forEach(jc -> modelBuilder.add(Rdf.Rml.joinCondition, jc.getAsResource()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CarmlRefObjectMap that = (CarmlRefObjectMap) o;
    return Objects.equal(parentTriplesMap.getId(), that.parentTriplesMap.getId())
        && Objects.equal(joinConditions, that.joinConditions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(parentTriplesMap.getId(), joinConditions);
  }

  @Override
  public String toString() {
    return "CarmlRefObjectMap(super=" + super.toString() + ", parentTriplesMap=" + this.getParentTriplesMap()
        .getAsResource()
        .stringValue() + ", joinConditions=" + this.getJoinConditions() + ")";
  }
}
