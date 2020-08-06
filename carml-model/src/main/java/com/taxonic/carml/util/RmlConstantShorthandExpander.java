package com.taxonic.carml.util;

import static com.taxonic.carml.vocab.Rdf.Rr.*;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Replaces RML constant shortcut properties by their expanded/full form.
 * See https://www.w3.org/TR/r2rml/#constant
 * The input model is not modified. A new model is created, populated
 * with the result and returned.
 * For the sake of keeping the implementation simple, all triples that
 * match the shortcut signatures are replaced, without consideration of
 * their position in the graph / relation to other objects.
 */
public class RmlConstantShorthandExpander implements UnaryOperator<Model> {

	private static final List<IRI> shortcutPredicates =
		Arrays.asList(subject, predicate, object, graph);
	
	private static final Map<IRI, IRI> expandedPredicates;
	
	private static final ValueFactory f = SimpleValueFactory.getInstance();
	
	static {
		class CreateExpandedPredicates {
			Map<IRI, IRI> expandedPredicates = new LinkedHashMap<>();
			
			void add(IRI shortcutPredicate, IRI expandedPredicate) {
				expandedPredicates.put(shortcutPredicate, expandedPredicate);
			}
			
			Map<IRI, IRI> run() {
				add(subject, subjectMap);
				add(predicate, predicateMap);
				add(object, objectMap);
				add(graph, graphMap);
				return ImmutableMap.copyOf(expandedPredicates);
			}
		}
		expandedPredicates = new CreateExpandedPredicates().run();
	}
	
	private IRI getExpandedPredicate(IRI shortcutPredicate) {
		if (!expandedPredicates.containsKey(shortcutPredicate))
			throw new IllegalArgumentException(
				"predicate [" + shortcutPredicate + "] is not a valid shortcut predicate");
		return expandedPredicates.get(shortcutPredicate);
	}
	
	@Override
	public Model apply(Model input) {
		
		Model model = new LinkedHashModel();
		input.forEach(statement -> expandStatements(model, statement));
		return model;
	}
	
	private void expandStatements(Model model, Statement statement) {
		IRI p = statement.getPredicate();
		
		// add statements that are NOT shortcut properties
		// as-is to the result model
		if (!shortcutPredicates.contains(p)) {
			model.add(statement);
			return;
		}
		
		// 'p' is a shortcut predicate
		Resource context = statement.getContext();
		BNode bNode = f.createBNode();
		// TODO verify that 'context' works properly, even if it is null
		model.add(statement.getSubject(), getExpandedPredicate(p), bNode, context);
		model.add(bNode, constant, statement.getObject(), context);
	}
}
