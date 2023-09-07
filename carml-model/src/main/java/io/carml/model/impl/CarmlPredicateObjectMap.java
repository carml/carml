package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.BaseObjectMap;
import io.carml.model.GraphMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.rdfmapper.annotations.RdfTypeDecider;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class CarmlPredicateObjectMap extends CarmlResource implements PredicateObjectMap {

  @Singular
  @Setter
  private Set<PredicateMap> predicateMaps;

  @Singular
  @Setter
  private Set<BaseObjectMap> objectMaps;

  @Singular
  @Setter
  private Set<GraphMap> graphMaps;

  @RdfProperty(Rr.predicateMap)
  @RdfType(CarmlPredicateMap.class)
  @Override
  public Set<PredicateMap> getPredicateMaps() {
    return predicateMaps;
  }

  @RdfProperty(Rr.objectMap)
  @RdfTypeDecider(ObjectMapTypeDecider.class)
  @Override
  public Set<BaseObjectMap> getObjectMaps() {
    return objectMaps;
  }

  @RdfProperty(Rr.graphMap)
  @RdfType(CarmlGraphMap.class)
  @Override
  public Set<GraphMap> getGraphMaps() {
    return graphMaps;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.<Resource>builder()
        .addAll(predicateMaps)
        .addAll(objectMaps)
        .addAll(graphMaps)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.PredicateObjectMap);

    predicateMaps.forEach(pm -> modelBuilder.add(Rr.predicateMap, pm.getAsResource()));
    objectMaps.forEach(om -> modelBuilder.add(Rr.objectMap, om.getAsResource()));
    graphMaps.forEach(gm -> modelBuilder.add(Rr.graphMap, gm.getAsResource()));
  }
}
