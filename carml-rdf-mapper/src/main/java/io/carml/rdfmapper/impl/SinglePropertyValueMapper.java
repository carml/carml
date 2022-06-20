package io.carml.rdfmapper.impl;

import java.util.List;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

class SinglePropertyValueMapper implements PropertyValueMapper {

  private IRI predicate;

  private ValueTransformer valueTransformer;

  public SinglePropertyValueMapper(IRI predicate, ValueTransformer valueTransformer) {
    this.predicate = predicate;
    this.valueTransformer = valueTransformer;
  }

  @Override
  public Optional<Object> map(Model model, Resource resource, Object instance, List<Value> values) {

    // multiple values present - error
    if (values.size() > 1) {
      throw new CarmlMapperException(String.format("multiple values for property <%s> on resource <%s>, but "
          + "corresponding java property is NOT an Iterable property", predicate, resource));
    }

    if (!values.isEmpty()) {
      Object result = valueTransformer.transform(model, values.get(0));
      return Optional.of(result);
    }

    return Optional.empty();
  }

}
