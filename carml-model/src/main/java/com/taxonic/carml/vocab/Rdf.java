package com.taxonic.carml.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class Rdf {
	
	private static final ValueFactory f = SimpleValueFactory.getInstance();
	
	private Rdf() {}
	
	public static class Rml {
		private static IRI iri(String suffix) {
			return f.createIRI(com.taxonic.carml.vocab.Rml.NAMESPACE + suffix);
		}
		
		public static final IRI logicalSource = iri("logicalSource");
		public static final IRI LogicalSource = iri("LogicalSource");
	}
	
	public static class Ql {
		private static final String NAMESPACE = "http://semweb.mmlab.be/ns/ql#";

		private static IRI iri(String suffix) {
			return f.createIRI(NAMESPACE + suffix);
		}

		public static final IRI JsonPath = iri("JSONPath");
		public static final IRI XPath = iri("XPath");
		public static final IRI Csv = iri("CSV");
	}
	
	public static class Rr {
		private static IRI iri(String suffix) {
			return f.createIRI(com.taxonic.carml.vocab.Rr.NAMESPACE + suffix);
		}

		public static final IRI subject = iri("subject");
		public static final IRI subjectMap = iri("subjectMap");
		public static final IRI predicate = iri("predicate");
		public static final IRI predicateMap = iri("predicateMap");
		public static final IRI object = iri("object");
		public static final IRI objectMap = iri("objectMap");
		public static final IRI graph = iri("graph");
		public static final IRI graphMap = iri("graphMap");
		public static final IRI constant = iri("constant");
		public static final IRI defaultGraph = iri("defaultGraph");

		public static final IRI parentTriplesMap = iri("parentTriplesMap");
		public static final IRI joinCondition = iri("joinCondition");
		public static final IRI child = iri("child");
		public static final IRI parent = iri("parent");

		public static final IRI TriplesMap = iri("TriplesMap");
		public static final IRI SubjectMap = iri("SubjectMap");
		public static final IRI PredicateMap = iri("PredicateMap");
		public static final IRI ObjectMap = iri("ObjectMap");
		public static final IRI PredicateObjectMap = iri("PredicateObjectMap");
		public static final IRI RefObjectMap = iri("RefObjectMap");
		public static final IRI GraphMap = iri("GraphMap");
		public static final IRI Join = iri("Join");

		public static final IRI Literal = iri("Literal");
		public static final IRI BlankNode = iri("BlankNode");
		public static final IRI IRI = iri("IRI");
	}

	public static class Fnml {
		private static IRI iri(String suffix) {
			return f.createIRI(com.taxonic.carml.vocab.Fnml.NAMESPACE + suffix);
		}

		public static final IRI functionValue = iri("functionValue");
	}
	
	public static class Fno {
		private static IRI iri(String suffix) {
			return f.createIRI(com.taxonic.carml.vocab.Fno.NAMESPACE + suffix);
		}
		public static final IRI Execution = iri("Execution");
		public static final IRI executes = iri("executes");
		public static final IRI old_executes = f.createIRI(com.taxonic.carml.vocab.Fno.OLD_executes);
	}	
	
	public static class Carml {
		private static IRI iri(String suffix) {
			return f.createIRI(com.taxonic.carml.vocab.Carml.NAMESPACE + suffix);
		}

		public static final IRI Stream = iri("Stream");
		public static final IRI streamName = iri("streamName");

		public static final IRI FileSource = iri("FileSource");
		public static final IRI url = iri("url");
		public static final IRI XmlDocument = iri("XmlDocument");
		public static final IRI declaresNamespace = iri("declaresNamespace");
		public static final IRI Namespace = iri("Namespace");
		public static final IRI namespacePrefix = iri("namespacePrefix");
		public static final IRI namespaceName = iri("namespaceName");

		public static final IRI MultiObjectMap = iri("MultiObjectMap");
		public static final IRI multiReference = iri("multiReference");
		public static final IRI multiTemplate = iri("multiTemplate");
		public static final IRI multiFunctionValue = iri("multiFunctionValue");
		public static final IRI multiJoinCondition = iri("multiJoinCondition");
	}	
	
}
