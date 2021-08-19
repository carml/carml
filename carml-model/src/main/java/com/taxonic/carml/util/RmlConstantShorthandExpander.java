package com.taxonic.carml.util;

import com.google.common.collect.ImmutableMap;
import com.taxonic.carml.vocab.Rdf.Rml;
import com.taxonic.carml.vocab.Rdf.Rr;
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
      Arrays.asList(Rr.subject, Rr.predicate, Rr.object, Rr.graph, Rr.datatype, Rr.language);

  private static final Map<IRI, IRI> expandedPredicates;

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  static {
    class CreateExpandedPredicates {
      final Map<IRI, IRI> expandedPredicates = new LinkedHashMap<>();

      void add(IRI shortcutPredicate, IRI expandedPredicate) {
        expandedPredicates.put(shortcutPredicate, expandedPredicate);
      }

      Map<IRI, IRI> run() {
        add(Rr.subject, Rr.subjectMap);
        add(Rr.predicate, Rr.predicateMap);
        add(Rr.object, Rr.objectMap);
        add(Rr.graph, Rr.graphMap);
        add(Rr.datatype, Rml.datatypeMap);
        add(Rr.language, Rml.languageMap);
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
    model.add(blankNode, Rr.constant, statement.getObject(), context);
  }
}
