package io.carml.rdfmapper.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.util.RDFContainers;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfCollectionPropertyValueMapper implements PropertyValueMapper {

    private final ValueTransformer valueTransformer;

    private final IRI rdfCollectionType;

    private final Collector<Object, ?, ? extends Collection<Object>> targetCollector;

    public static RdfCollectionPropertyValueMapper of(
            ValueTransformer valueTransformer, IRI rdfCollectionType, Class<?> iterableType) {
        Collector<Object, ?, ? extends Collection<Object>> targetCollector;
        if (iterableType.equals(List.class)) {
            targetCollector = Collectors.toUnmodifiableList();
        } else if (iterableType.equals(Set.class)) {
            targetCollector = Collectors.toUnmodifiableSet();
        } else {
            throw new CarmlMapperException(
                    String.format("Unsupported target collection type for a RDF " + "collection: %s", iterableType));
        }

        return new RdfCollectionPropertyValueMapper(valueTransformer, rdfCollectionType, targetCollector);
    }

    @Override
    public Optional<Object> map(Model model, Resource resource, Object instance, List<Value> values) {
        if (rdfCollectionType.equals(RDF.LIST)) {
            var listStatements = new LinkedHashModel();
            RDFCollections.getCollection(model, (Resource) values.get(0), listStatements);

            var collectionResult = listStatements.filter(null, RDF.FIRST, null).stream()
                    .map(Statement::getObject)
                    .map(value -> valueTransformer.transform(model, value))
                    .collect(targetCollector);

            return Optional.of(collectionResult);
        } else {
            var containerStatements = new ArrayList<Statement>();
            RDFContainers.extract(rdfCollectionType, model, (Resource) values.get(0), containerStatements::add);

            // RDF4J container extraction ignores rdfs:member statements
            containerStatements.addAll(model.filter(null, RDFS.MEMBER, null));

            var collectionResult = containerStatements.stream()
                    .map(Statement::getObject)
                    .map(value -> valueTransformer.transform(model, value))
                    .collect(targetCollector);

            return Optional.of(collectionResult);
        }
    }
}
