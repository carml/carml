package io.carml.rdfmapper.impl;

import io.carml.rdfmapper.Mapper;
import io.carml.rdfmapper.TypeDecider;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuppressWarnings("java:S1135")
public class TypeFromTripleTypeDecider implements TypeDecider {

  private final Mapper mapper;

  private final Optional<TypeDecider> propertyTypeDecider;

  public TypeFromTripleTypeDecider(Mapper mapper) {
    this(mapper, Optional.empty());
  }

  public TypeFromTripleTypeDecider(Mapper mapper, Optional<TypeDecider> propertyTypeDecider) {
    this.mapper = mapper;
    this.propertyTypeDecider = propertyTypeDecider;
  }

  @Override
  public Set<Type> decide(Model model, Resource resource) {

    List<IRI> rdfTypes = model.filter(resource, RDF.TYPE, null)
        .objects()
        .stream()
        .map(IRI.class::cast)
        .toList();

    // TODO what if multiple rdf:types? probably choose the only 1 that's known/registered. what if
    // multiple of those?
    if (rdfTypes.size() > 1) {
      return rdfTypes.stream()
          .map(mapper::getDecidableType)
          .collect(Collectors.toSet());
    }

    // if no rdf:type, use property type (or its registered implementation) as target type
    if (rdfTypes.isEmpty()) {
      if (propertyTypeDecider.isPresent()) {
        return propertyTypeDecider.get()
            .decide(model, resource);
      } else {
        throw new CarmlMapperException(
            String.format("No decidable type found for %s. Register decidable type on rdf mapper.", resource));
      }
    }

    IRI rdfType = rdfTypes.get(0);
    return Set.of(mapper.getDecidableType(rdfType));
  }

}
