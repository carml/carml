package com.taxonic.rml.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class Rdf {
	
	private static final ValueFactory f = SimpleValueFactory.getInstance();
	
	public static class Rml {
		
		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://semweb.mmlab.be/ns/rml#";
		
		public static final IRI logicalSource = iri("logicalSource");
		
	}
	
	public static class Ql {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://semweb.mmlab.be/ns/ql#";
		
		public static final IRI JsonPath = iri("JSONPath");
		
	}
	
	public static class Rr {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://www.w3.org/ns/r2rml#";

		public static final IRI
		
			subject      = iri("subject"     ),
			subjectMap   = iri("subjectMap"  ),
			predicate    = iri("predicate"   ),
			predicateMap = iri("predicateMap"),
			object       = iri("object"      ),
			objectMap    = iri("objectMap"   ),
			graph        = iri("graph"       ),
			graphMap     = iri("graphMap"    ),
			constant     = iri("constant"    ),

			parentTriplesMap = iri("parentTriplesMap"),
			joinCondition    = iri("joinCondition"   ),
			child            = iri("child"           ),
			parent           = iri("parent"          ),

			Literal		 = iri("Literal"	 ),
			BlankNode	 = iri("BlankNode"	 ),
			IRI			 = iri("IRI"	 	 );
		
	}
}
