package com.taxonic.carml.vocab;

public class Rr {

	public static final String
	
		prefix = "http://www.w3.org/ns/r2rml#",
		
		clazz = prefix + "class",
		language = prefix + "language",
		datatype = prefix + "datatype",
		template = prefix + "template",
		predicate = prefix + "predicate",
		predicateMap = prefix + "predicateMap",
		subject = prefix + "subject",
		subjectMap = prefix + "subjectMap",
		object = prefix + "object",
		objectMap = prefix + "objectMap",
		inverseExpression = prefix + "inverseExpression",
		//TODO Decide whether instead of termType --> dfn-term-type?
		termType = prefix + "termType",
		constant = prefix + "constant",
		predicateObjectMap = prefix + "predicateObjectMap",
		graphMap = prefix + "graphMap",

		TriplesMap 			 = prefix + "TriplesMap",
		parentTriplesMap = prefix + "parentTriplesMap",
		joinCondition    = prefix + "joinCondition",
		child            = prefix + "child",
		parent           = prefix + "parent",
	
		IRI = prefix + "IRI",
		BLANK_NODE = prefix + "BlankNode",
		LITERAL = prefix + "Literal";
	
}
