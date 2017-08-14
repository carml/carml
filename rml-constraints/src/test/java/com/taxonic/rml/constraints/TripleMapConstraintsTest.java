package com.taxonic.rml.constraints;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.junit.Test;
import org.topbraid.shacl.validation.ValidationUtil;

public class TripleMapConstraintsTest {

	public static void main(String... args) {

		String common = "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
				"@prefix sh:   <http://www.w3.org/ns/shacl#> .\n" +
				"@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .\n" +
				"@prefix rml:  <http://www.w3.org/2001/RML#> .\n" +
				"\n" +
				"\n" +
				"@prefix rmlsh: <http://www.taxonic.com/ns/rml-shacl#> .\n";
		Model data = makeModel(
				common +
				"rml:tripleMap\n" +
				"\trml:logicalSource \"cheesypotatoes\"@en .\n");
		Model shapes = makeModel(
				common +
						"\n" +
						"rmlsh:TripleMapShape\n" +
						"\ta sh:NodeShape ;\n" +
						"\trdfs:label \"Triple map shape\"@en ;\n" +
						"\trdfs:comment \"Defines constraints describing a well-formed RML triple map.\"@en ;" +
						"\tsh:targetSubjectsOf rml:logicalSource ;" +
						"\tsh:property [ sh:path rml:subjectMap; sh:minCount 1 ; ] ." +
						"\n");

		shapes = loadModel("rml.sh.ttl");

		Resource result = ValidationUtil.validateModel(data, shapes, false);
		StringWriter w = new StringWriter();

		result.getModel().write(w, "TURTLE");
		String s = w.toString();
		int debug = 10;




//		ValidationEngineFactory f = ValidationEngineFactory.get();
//		ValidationEngine validationEngine = f.create(data, null, null, null);

//		validationEngine.getShapesGraph().setShapeFilter();

	}

	private static Model loadModel(String name) {
		try (InputStream input = TripleMapConstraintsTest.class.getClassLoader().getResourceAsStream(name)) {
			return ModelFactory.createDefaultModel()
					.read(input, "http://none.com/", "TURTLE");
		}
		catch (IOException e) {
			throw new RuntimeException("failed to load model from resource [" + name + "]", e);
		}
	}

	private static Model makeModel(String data) {
		return ModelFactory.createDefaultModel().read(new StringReader(data), "http://a.b/", "TURTLE");
	}

	@Test
	public void constraintsTriggerByClass() {

	}

	@Test
	public void constraintsTriggerOnLogicalSourceSubject() {

	}

	@Test
	public void constraintsTriggerOnSubjectMapSubject() {

	}
}
