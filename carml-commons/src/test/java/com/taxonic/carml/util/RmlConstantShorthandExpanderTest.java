package com.taxonic.carml.util;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.taxonic.carml.vocab.Rdf.Rr;

public class RmlConstantShorthandExpanderTest {

	private static final RmlConstantShorthandExpander SHORTHAND_EXPANDER = new RmlConstantShorthandExpander();
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String NS = "http://test.org/";
	private static final IRI TM = VF.createIRI(NS + "someTriplesMap");
	private static final IRI SM = VF.createIRI(NS + "someSubjectMap");
	private static final IRI POM = VF.createIRI(NS + "somePredicateObjectMap");
	
	Model model;
	
	@Before
	public void setupModel() {
		model = new LinkedHashModel();
	}
	
	@Test
	public void expander_givenSubjectShorthand_ShouldExpandCorrectly() {
		Statement toExpand = VF.createStatement(TM, Rr.subject, SM);
		expandAndTestShorthandStatement(toExpand, Rr.subjectMap);
	}
	
	@Test
	public void expander_givenPredicateShorthand_ShouldExpandCorrectly() {
		IRI predicateMapValue = VF.createIRI(NS + "somePredicateMap");
		Statement toExpand = VF.createStatement(POM, Rr.predicate, predicateMapValue);
		expandAndTestShorthandStatement(toExpand, Rr.predicateMap);
	}
	
	@Test
	public void expander_givenObjectShorthand_ShouldExpandCorrectly() {
		IRI objectMapValue = VF.createIRI(NS + "someObjectMap");
		Statement toExpand = VF.createStatement(POM, Rr.object, objectMapValue);
		expandAndTestShorthandStatement(toExpand, Rr.objectMap);
	}
	
	@Test
	public void expander_givenGraphShorthand_ShouldExpandCorrectly() {
		IRI graphMapValue = VF.createIRI(NS + "someGraphMap");
		Statement toExpand = VF.createStatement(SM, Rr.graph, graphMapValue);
		expandAndTestShorthandStatement(toExpand, Rr.graphMap);
	}
	
	@Test
	public void expander_givenShorthandWithContext_ShouldHaveContextOnExpandedStatements() {
		IRI context = VF.createIRI(NS + "someContext");
		Statement toExpand = VF.createStatement(TM, Rr.subject, SM, context);
		Model expanded = expandAndTestShorthandStatement(toExpand, Rr.subjectMap);
		
		int nrOfstatementsWithContext = 
			(int) expanded
				.stream()
				.filter(st -> st.getContext() != null)
				.count();
		
		assertThat(nrOfstatementsWithContext, is(2));
		
		assertThat(expanded.contexts().size(), is(1));
		assertThat(expanded.contexts(), hasItem(context));
	}
	
	private Model expandAndTestShorthandStatement(Statement toExpand, IRI expandedPredicate) {
		model.add(toExpand);
		Model expanded = SHORTHAND_EXPANDER.apply(model);
		assertThat(expanded.size(), is(2));
		
		Statement firstStatement = 
			Iterators.getOnlyElement(
				expanded
					.filter(null, expandedPredicate, null)
					.iterator()
			); 
		assertThat(firstStatement.getSubject(), is(toExpand.getSubject()));
		assertThat(firstStatement.getObject(), instanceOf(BNode.class));
		
		Statement otherSt = 
			Iterators.getOnlyElement(
				expanded
					.filter((Resource)firstStatement.getObject(), null, null)
					.iterator()
			);
		assertThat(otherSt.getPredicate(), is(Rr.constant));
		assertThat(otherSt.getObject(), is(toExpand.getObject()));
		
		return expanded;
	}
}
