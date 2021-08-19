package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.DatatypeMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rml;
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
