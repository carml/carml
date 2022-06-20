package io.carml.util;

import com.google.common.collect.ImmutableMap;
import io.carml.vocab.Rdf;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
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
 * https://www.w3.org/TR/r2rml/#constant The input model is not modified. A new model is created,
 * populated with the result and returned. For the sake of keeping the implementation simple, all
 * triples that match the shortcut signatures are replaced, without consideration of their position
 * in the graph / relation to other objects.
 */
public class RmlConstantShorthandExpander implements UnaryOperator<Model> {

  private static final List<IRI> shortcutPredicates =
      Arrays.asList(Rdf.Rr.subject, Rdf.Rr.predicate, Rdf.Rr.object, Rdf.Rr.graph, Rdf.Rr.datatype, Rdf.Rr.language);

  private static final Map<IRI, IRI> expandedPredicates;

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  static {
    class CreateExpandedPredicates {
      final Map<IRI, IRI> expandedPredicates = new LinkedHashMap<>();

      void add(IRI shortcutPredicate, IRI expandedPredicate) {
        expandedPredicates.put(shortcutPredicate, expandedPredicate);
      }

      Map<IRI, IRI> run() {
        add(Rdf.Rr.subject, Rdf.Rr.subjectMap);
        add(Rdf.Rr.predicate, Rdf.Rr.predicateMap);
        add(Rdf.Rr.object, Rdf.Rr.objectMap);
        add(Rdf.Rr.graph, Rdf.Rr.graphMap);
        add(Rdf.Rr.datatype, Rdf.Rml.datatypeMap);
        add(Rdf.Rr.language, Rdf.Rml.languageMap);
        return ImmutableMap.copyOf(expandedPredicates);
      }
    }

    expandedPredicates = new CreateExpandedPredicates().run();
  }

  private IRI getExpandedPredicate(IRI shortcutPredicate) {
    if (!expandedPredicates.containsKey(shortcutPredicate)) {
      throw new IllegalArgumentException(
          String.format("predicate [%s] is not a valid shortcut predicate", shortcutPredicate));
    }

    return expandedPredicates.get(shortcutPredicate);
  }

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
    if (!shortcutPredicates.contains(shortcutPredicate)) {
      model.add(statement);
      return;
    }

    Resource context = statement.getContext();
    BNode blankNode = VALUE_FACTORY.createBNode();
    model.add(statement.getSubject(), getExpandedPredicate(shortcutPredicate), blankNode, context);
    model.add(blankNode, Rdf.Rr.constant, statement.getObject(), context);
  }
}
