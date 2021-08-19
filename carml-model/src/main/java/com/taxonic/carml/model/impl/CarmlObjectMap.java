package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.DatatypeMap;
import com.taxonic.carml.model.LanguageMap;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.rdfmapper.annotations.RdfType;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlObjectMap extends CarmlTermMap implements ObjectMap {

  @Setter
  private DatatypeMap datatypeMap;

  @Setter
  private LanguageMap languageMap;

  @RdfProperty(Rml.datatypeMap)
  @RdfType(CarmlDatatypeMap.class)
  @Override
  public DatatypeMap getDatatypeMap() {
    return datatypeMap;
  }

  @RdfProperty(Rml.languageMap)
  @RdfType(CarmlLanguageMap.class)
  @Override
  public LanguageMap getLanguageMap() {
    return languageMap;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
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
        .add(RDF.TYPE, Rdf.Rr.ObjectMap);

    addTriplesBase(modelBuilder);

    if (datatypeMap != null) {
      modelBuilder.add(Rml.datatypeMap, datatypeMap.getAsResource());
    }
    if (languageMap != null) {
      modelBuilder.add(Rml.languageMap, languageMap.getAsResource());
    }
  }
}
