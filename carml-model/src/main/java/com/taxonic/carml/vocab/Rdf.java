package com.taxonic.carml.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class Rdf {
	
	private static final ValueFactory f = SimpleValueFactory.getInstance();
	
	private Rdf() {}
	
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
		
		public static final IRI 
			
			JsonPath 	 = iri("JSONPath"	 ),
			XPath 	 	 = iri("XPath"		 ),
			Csv 	 	 = iri("CSV"		 );
		
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

	public static class Fnml {

		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://semweb.mmlab.be/ns/fnml#";
		
		public static final IRI
		
			functionValue = iri("functionValue");
		
	}
	
	public static class Fno {
		
		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://semweb.datasciencelab.be/ns/function#";
		
		public static final IRI
		
			Execution = iri("Execution"),
			executes = iri("executes");
		
	}	
	
	public static class Carml {
		
		static IRI iri(String suffix) {
			return f.createIRI(prefix + suffix);
		}
		
		public static final String prefix = "http://carml.taxonic.com/carml/";
		
		public static final IRI
		
			Stream = iri("Stream"),
			streamName = iri("streamName"),

			url = iri("url"),
			XmlDocument = iri("XmlDocument"),
			declaresNamespace = iri("declaresNamespace"),
			Namespace = iri("Namespace"),
			namespacePrefix = iri("namespacePrefix"),
			namespaceName = iri("namespaceName"),
		
			MultiObjectMap = iri("MultiObjectMap"),
			multiReference = iri("multiReference"),
			multiTemplate = iri("multiTemplate"),
			multiFunctionValue = iri("multiFunctionValue"),
		
			multiJoinCondition = iri("multiJoinCondition");
		
	}	
	
}
