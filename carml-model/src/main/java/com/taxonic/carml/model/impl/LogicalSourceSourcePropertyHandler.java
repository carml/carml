package com.taxonic.carml.model.impl;

import com.taxonic.carml.rdfmapper.Mapper;
import com.taxonic.carml.rdfmapper.PropertyHandler;
import com.taxonic.carml.rdfmapper.impl.CarmlMapperException;
import com.taxonic.carml.rdfmapper.impl.ComplexValueTransformer;
import com.taxonic.carml.rdfmapper.impl.MappingCache;
import com.taxonic.carml.rdfmapper.qualifiers.PropertyPredicate;
import com.taxonic.carml.rdfmapper.qualifiers.PropertySetter;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.inject.Inject;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

@SuppressWarnings("java:S1135")
public class LogicalSourceSourcePropertyHandler implements PropertyHandler {

  private IRI predicate;

  private BiConsumer<Object, Object> setter;

  private Mapper mapper;

  private MappingCache mappingCache;

  private Optional<Object> determineValue(Model model, Resource resource) {

    Set<Value> objects = model.filter(resource, predicate, null)
        .objects();
    if (objects.size() > 1) {
      throw new CarmlMapperException(
          String.format("more than 1 object for the predicate [%s] for a logical source", predicate));
    }

    if (objects.isEmpty()) {
      return Optional.empty();
    }

    Value object = objects.iterator()
        .next();

    if (object instanceof Literal) {
      return Optional.of(object.stringValue());
    }

    // map 'object' to some complex type
    // TODO quite nasty to create the transformer here
    ComplexValueTransformer transformer =
        new ComplexValueTransformer(new LogicalSourceSourceTypeDecider(mapper), mappingCache, mapper, o -> o);
    Object value = transformer.transform(model, object);
    return Optional.of(value);
  }

  @Override
  public void handle(Model model, Resource resource, Object instance) {

    determineValue(model, resource)

        .ifPresent(value -> setter.accept(instance, value));
  }

  @Override
  public boolean hasEffect(Model model, Resource resource) {
    return !model.filter(resource, predicate, null)
        .isEmpty();
  }

  @Inject
  @PropertyPredicate
  public void setPredicate(IRI predicate) {
    this.predicate = predicate;
  }

  @Inject
  @PropertySetter
  public void setSetter(BiConsumer<Object, Object> setter) {
    this.setter = setter;
  }

  @Inject
  public void setMapper(Mapper mapper) {
    this.mapper = mapper;
  }

  @Inject
  public void setMappingCache(MappingCache mappingCache) {
    this.mappingCache = mappingCache;
  }

}
