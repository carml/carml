package com.taxonic.carml.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

public class ModelUtil {

    private ModelUtil() {}

    public static Model describeResource(Model model, Resource resource) {
        return model.filter(resource, null, null)
                .stream()
                .flatMap(statement -> {
                    if (statement.getObject() instanceof BNode) {
                        return Stream.concat(Stream.of(statement), describeResource(model, (BNode) statement.getObject()).stream());
                    } else {
                        return Stream.of(statement);
                    }
                })
                .collect(Collectors.toCollection(LinkedHashModel::new));
    }

    public static Model reverseDescribeResource(Model model, Resource resource) {
        return model.filter(null, null, resource)
                .stream()
                .flatMap(statement -> {
                    if (statement.getSubject() instanceof BNode) {
                        return Stream.concat(Stream.of(statement), reverseDescribeResource(model, statement.getSubject()).stream());
                    } else {
                        return Stream.of(statement);
                    }
                })
                .collect(Collectors.toCollection(LinkedHashModel::new));
    }

    public static Model symmetricDescribeResource(Model model, Resource resource) {
        Model description = reverseDescribeResource(model, resource);
        description.addAll(describeResource(model, resource));
        return description;
    }
}
