package io.carml.model.impl;

import io.carml.model.impl.template.TemplateParser;
import io.carml.rdfmapper.PropertyHandler;
import io.carml.rdfmapper.impl.CarmlMapperException;
import io.carml.rdfmapper.qualifiers.PropertyPredicate;
import io.carml.rdfmapper.qualifiers.PropertySetter;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.inject.Inject;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

public class TemplatePropertyHandler implements PropertyHandler {

    private IRI predicate;

    private BiConsumer<Object, Object> setter;

    @Override
    public void handle(Model model, Resource resource, Object instance) {
        determineValue(model, resource).ifPresent(value -> setter.accept(instance, value));
    }

    @Override
    public boolean hasEffect(Model model, Resource resource) {
        return !model.filter(resource, predicate, null).isEmpty();
    }

    private Optional<Object> determineValue(Model model, Resource resource) {
        Set<Value> objects = model.filter(resource, predicate, null).objects();
        if (objects.size() > 1) {
            throw new CarmlMapperException(
                    String.format("more than 1 object for the predicate %s for resource:%n%s", predicate, resource));
        }

        if (objects.isEmpty()) {
            return Optional.empty();
        }

        Value object = objects.iterator().next();

        if (object instanceof Literal literal) {
            var templateString = literal.stringValue();

            return Optional.of(TemplateParser.getInstance().parse(templateString));
        }

        throw new CarmlMapperException(
                String.format("object for the predicate %s for resource:%n%s is not a literal", predicate, resource));
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
}
