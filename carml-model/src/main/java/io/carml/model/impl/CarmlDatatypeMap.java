package io.carml.model.impl;

import io.carml.model.DatatypeMap;
import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlDatatypeMap extends CarmlExpressionMap implements DatatypeMap {

  public CarmlDatatypeMap(String id, String label, String reference, String template, Value constant,
      TriplesMap functionValue) {
    super(id, label, reference, template, constant, functionValue);
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return getReferencedResourcesBase();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rml.DatatypeMap);

    addTriplesBase(modelBuilder);
  }
}
