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

		public static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://semweb.mmlab.be/ns/ql#";
		
		public static final IRI JsonPath = iri("JSONPath");
		
	}
}
