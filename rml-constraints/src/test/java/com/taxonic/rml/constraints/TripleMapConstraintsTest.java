package com.taxonic.rml.constraints;


import static org.apache.logging.log4j.core.util.Loader.getClassLoader;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Selector;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.junit.Assert;
import org.junit.Test;
import org.topbraid.shacl.validation.ValidationUtil;

public class TripleMapConstraintsTest {

	public static void main(String... args) throws IOException {


		Model shapes = loadModel("rml.sh.ttl");

		Collection<String> files =
				IOUtils.readLines(TripleMapConstraintsTest.class.getClassLoader()
						.getResourceAsStream("testMappings/"), Charsets.UTF_8);

		String shconforms = "http://www.w3.org/ns/shacl#conforms";

		files.stream().forEach(f -> {
			System.out.printf("loading %s\n", "testMappings/" + f);
			Model data = loadModel("testMappings/" + f);
			Resource result = ValidationUtil.validateModel(data, shapes, false);

			NodeIterator nodeIterator = result.getModel().listObjectsOfProperty(new PropertyImpl(shconforms));

			StringBuilder resultThing = new StringBuilder();
			List<Boolean> conforms = new ArrayList<>(1);
			nodeIterator.forEachRemaining(n -> conforms.add(n.asLiteral().getBoolean()));

			Assert.assertEquals(1, conforms.size());
			Assert.assertFalse(f, conforms.get(0));

		});

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
