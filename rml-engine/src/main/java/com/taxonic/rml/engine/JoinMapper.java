package com.taxonic.rml.engine;

import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.Join;

public class JoinMapper {
	
	Set<Join> joinConditions;
	
	public JoinMapper(Set<Join> joinConditions) {
		super();
		this.joinConditions = joinConditions;
	}

	void map(Model model, EvaluateExpression evaluate, Resource subject, Resource... contexts) {
	}
}
