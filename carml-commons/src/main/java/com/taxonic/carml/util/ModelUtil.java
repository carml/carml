package com.taxonic.carml.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

public class ModelUtil {

    private ModelUtil() {}

    public static Model describeResource(Model model, Resource resource) {
        return model.filter(resource, null, null)
                .stream()
                .flatMap(statement -> {
                    Value object = statement.getObject();
                    return object instanceof BNode ?
                            Stream.concat(Stream.of(statement), describeResource(model, (BNode) object).stream()) :
                            Stream.of(statement);
                })
                .collect(Collectors.toCollection(LinkedHashModel::new));
    }

    public static Model reverseDescribeResource(Model model, Resource resource) {
        return model.filter(null, null, resource)
                .stream()
                .flatMap(statement -> {
                    Resource subject = statement.getSubject();
                    return subject instanceof BNode ?
                            Stream.concat(Stream.of(statement), reverseDescribeResource(model, subject).stream()) :
                            Stream.of(statement);
                })
                .collect(Collectors.toCollection(LinkedHashModel::new));
    }

    public static Model symmetricDescribeResource(Model model, Resource resource) {
        Model description = reverseDescribeResource(model, resource);
        description.addAll(describeResource(model, resource));
        return description;
    }
}
