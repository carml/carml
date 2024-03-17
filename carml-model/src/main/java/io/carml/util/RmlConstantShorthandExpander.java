package io.carml.util;

import static java.util.Map.entry;

import io.carml.vocab.Rdf.OldRml;
import io.carml.vocab.Rdf.Rml;
import io.carml.vocab.Rdf.Rr;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Replaces RML constant shortcut properties by their expanded/full form. See
 * <a href="http://w3id.org/rml/core/spec#shortcuts-for-constant-valued-term-maps">Shortcuts for
 * constant-valued term maps</a> and <a href="http://w3id.org/rml/core/spec#shortcuts">Join
 * shortcuts</a>. The input model is not modified. A new model is created, populated with the result
 * and returned. For the sake of keeping the implementation simple, all triples that match the
 * shortcut signatures are replaced, without consideration of their position in the graph / relation
 * to other objects.
 */
public class RmlConstantShorthandExpander implements UnaryOperator<Model> {

    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

    private static final Map<IRI, PredicateExpansion> EXPANDED_PREDICATES = Map.ofEntries(
            entry(Rml.subject, new PredicateExpansion(Rml.subjectMap, Rml.constant)),
            entry(Rr.subject, new PredicateExpansion(Rr.subjectMap, Rr.constant)),
            entry(Rml.predicate, new PredicateExpansion(Rml.predicateMap, Rml.constant)),
            entry(Rr.predicate, new PredicateExpansion(Rr.predicateMap, Rr.constant)),
            entry(Rml.object, new PredicateExpansion(Rml.objectMap, Rml.constant)),
            entry(Rr.object, new PredicateExpansion(Rr.objectMap, Rr.constant)),
            entry(Rml.graph, new PredicateExpansion(Rml.graphMap, Rml.constant)),
            entry(Rr.graph, new PredicateExpansion(Rr.graphMap, Rr.constant)),
            entry(Rml.datatype, new PredicateExpansion(Rml.datatypeMap, Rml.constant)),
            entry(Rr.datatype, new PredicateExpansion(OldRml.datatypeMap, Rr.constant)),
            entry(Rml.language, new PredicateExpansion(Rml.languageMap, Rml.constant)),
            entry(Rr.language, new PredicateExpansion(OldRml.languageMap, Rr.constant)),
            entry(Rml.child, new PredicateExpansion(Rml.childMap, Rml.reference)),
            entry(Rr.child, new PredicateExpansion(Rml.childMap, Rml.reference)),
            entry(Rml.parent, new PredicateExpansion(Rml.parentMap, Rml.reference)),
            entry(Rr.parent, new PredicateExpansion(Rml.parentMap, Rml.reference)));

    private record PredicateExpansion(IRI expandedPredicate, IRI expressionPredicate) {}

    @Override
    public Model apply(Model input) {

        Model model = new LinkedHashModel();
        input.forEach(statement -> expandStatements(model, statement));
        return model;
    }

    private void expandStatements(Model model, Statement statement) {
        IRI shortcutPredicate = statement.getPredicate();

        // add statements that are NOT shortcut properties
        // as-is to the result model
        if (!EXPANDED_PREDICATES.containsKey(shortcutPredicate)) {
            model.add(statement);
            return;
        }

        var expansion = getExpandedPredicate(shortcutPredicate);
        var expandedPredicate = expansion.expandedPredicate;
        var expressionPredicate = expansion.expressionPredicate;

        Resource context = statement.getContext();
        BNode blankNode = VALUE_FACTORY.createBNode();
        model.add(statement.getSubject(), expandedPredicate, blankNode, context);
        model.add(blankNode, expressionPredicate, statement.getObject(), context);
    }

    private PredicateExpansion getExpandedPredicate(IRI shortcutPredicate) {
        if (!EXPANDED_PREDICATES.containsKey(shortcutPredicate)) {
            throw new IllegalArgumentException(
                    String.format("predicate [%s] is not a valid shortcut predicate", shortcutPredicate));
        }

        return EXPANDED_PREDICATES.get(shortcutPredicate);
    }
}
