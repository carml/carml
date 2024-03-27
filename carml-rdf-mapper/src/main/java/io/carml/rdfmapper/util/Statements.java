package io.carml.rdfmapper.util;

import static org.eclipse.rdf4j.model.util.Values.iri;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;

@UtilityClass
public class Statements {

    private static final Pattern ANNOTATED_MEMBERSHIP_PREDICATE_PATTERN =
            Pattern.compile("^" + iri(RDF.NAMESPACE, "_") + "[1-9][0-9]*$");

    private static final Set<Value> CONTAINER_TYPES = Set.of(RDF.ALT, RDF.BAG, RDF.SEQ);

    public static boolean isListStatement(Statement statement) {
        var predicate = statement.getPredicate();
        var object = statement.getObject();

        return (predicate.equals(RDF.TYPE) && object.equals(RDF.LIST))
                || predicate.equals(RDF.FIRST)
                || predicate.equals(RDF.REST);
    }

    public static boolean isContainerStatement(Statement statement) {
        var predicate = statement.getPredicate();

        return predicate.equals(RDFS.MEMBER)
                || ANNOTATED_MEMBERSHIP_PREDICATE_PATTERN
                        .matcher(predicate.stringValue())
                        .matches()
                || CONTAINER_TYPES.contains(statement.getObject());
    }

    public static Optional<IRI> getContainerTypeFor(Resource containerResource, Model model) {
        return model.filter(containerResource, RDF.TYPE, null).objects().stream()
                .filter(IRI.class::isInstance)
                .map(IRI.class::cast)
                .filter(CONTAINER_TYPES::contains)
                .findFirst();
    }

    public static Optional<IRI> getCollectionTypeFor(Resource collectionResource, Model model) {
        if (model.iterator().hasNext()) {
            var statement = model.iterator().next();
            if (isListStatement(statement)) {
                return Optional.of(RDF.LIST);
            } else if (isContainerStatement(statement)) {
                return getContainerTypeFor(collectionResource, model);
            }
        }

        return Optional.empty();
    }
}
