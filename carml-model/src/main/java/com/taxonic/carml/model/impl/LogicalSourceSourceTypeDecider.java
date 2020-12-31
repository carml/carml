package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableMap;
import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.vocab.Rdf;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class LogicalSourceSourceTypeDecider implements TypeDecider {

  private Mapper mapper;

  public LogicalSourceSourceTypeDecider(Mapper mapper) {
    this.mapper = mapper;
  }

  Map<IRI, IRI> inferenceMap = ImmutableMap.of(Rdf.Carml.streamName, Rdf.Carml.Stream, Rdf.Carml.declaresNamespace,
      Rdf.Carml.XmlDocument, Rdf.Carml.url, Rdf.Carml.FileSource);

  @Override
  public Set<Type> decide(Model model, Resource resource) {
    Set<IRI> rdfTypes = model.filter(resource, RDF.TYPE, null)
        .objects()
        .stream()
        .map(v -> (IRI) v)
        .collect(Collectors.toSet());

    Set<IRI> usedPredicates = model.filter(resource, null, null)
        .predicates()
        .stream()
        .filter(p -> !p.equals(RDF.TYPE))
        .collect(Collectors.toSet());

    usedPredicates.forEach(p -> {
      if (inferenceMap.containsKey(p)) {
        rdfTypes.add(inferenceMap.get(p));
      }
    });

    return rdfTypes.stream()
        .map(mapper::getDecidableType)
        .collect(Collectors.toSet());


  }

}
