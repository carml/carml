package io.carml.engine.rdf.util;

import java.util.List;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@UtilityClass
public class RdfCollectionsAndContainers {

    public static Model toRdfListModel(
            List<Value> values, Resource head, ValueFactory valueFactory, Resource... contexts) {
        return toRdfListModel(values, head, null, valueFactory, contexts);
    }

    public static Model toRdfListModel(
            List<Value> values, Resource head, Model valueModel, ValueFactory valueFactory, Resource... contexts) {
        Resource current = head;
        var iterator = values.iterator();
        var model = valueModel == null ? new LinkedHashModel() : new LinkedHashModel(valueModel);
        Consumer<Statement> consumer = model::add;

        while (iterator.hasNext()) {
            var value = iterator.next();
            Statements.consume(valueFactory, current, RDF.FIRST, value, consumer, contexts);
            if (iterator.hasNext()) {
                Resource next = valueFactory.createBNode();
                Statements.consume(valueFactory, current, RDF.REST, next, consumer, contexts);
                current = next;
            } else {
                Statements.consume(valueFactory, current, RDF.REST, RDF.NIL, consumer, contexts);
            }
        }

        return model;
    }

    public static Model toRdfContainerModel(
            IRI containerType,
            List<Value> values,
            Resource container,
            ValueFactory valueFactory,
            Resource... contexts) {
        return toRdfContainerModel(containerType, values, container, null, valueFactory, contexts);
    }

    public static Model toRdfContainerModel(
            IRI containerType,
            List<Value> values,
            Resource container,
            Model valueModel,
            ValueFactory valueFactory,
            Resource... contexts) {
        var model = valueModel == null ? new LinkedHashModel() : new LinkedHashModel(valueModel);
        Consumer<Statement> consumer = model::add;

        Statements.consume(valueFactory, container, RDF.TYPE, containerType, consumer, contexts);
        var iterator = values.iterator();
        int elementCounter = 1;
        while (iterator.hasNext()) {
            var value = iterator.next();
            var elementCounterPredicate = valueFactory.createIRI(RDF.NAMESPACE, "_" + elementCounter);
            elementCounter++;
            Statements.consume(valueFactory, container, elementCounterPredicate, value, consumer, contexts);
        }

        return model;
    }
}
