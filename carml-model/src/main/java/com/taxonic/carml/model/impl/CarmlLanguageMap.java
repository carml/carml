package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.LanguageMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.vocab.Rml;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlLanguageMap extends CarmlExpressionMap implements LanguageMap {

  @Override
  public Set<Resource> getReferencedResources() {
    return getReferencedResourcesBase();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rml.LanguageMap);

    addTriplesBase(modelBuilder);
  }
}
