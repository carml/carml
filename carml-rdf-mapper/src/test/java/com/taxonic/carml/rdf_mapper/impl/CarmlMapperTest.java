package com.taxonic.carml.rdf_mapper.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Test;

import com.github.jsonldjava.shaded.com.google.common.collect.ImmutableSet;

public class CarmlMapperTest {

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private Model model;

	public void prepareTest() {
		try (InputStream input = CarmlMapperTest.class.getResourceAsStream("Person.ld.json")) {
			model = Rio.parse(input, "", RDFFormat.JSONLD);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void mapper_havingMethodWithMultiplePropertyAnnotations_MapsCorrectly() {
		prepareTest();
		CarmlMapper mapper = new CarmlMapper();
		Person manu = mapper.map(model, (Resource) VF.createIRI("http://example.org/people#manu"),
				ImmutableSet.of(Person.class));
		Set<Person> acquaintances = manu.getKnows();
		assertThat(acquaintances, hasSize(6));
	}

	public void mapper_ifContainsCachedMappings_UsesThem() {
		//TODO
	}

	//TODO: PM: test mapping of relevant RDF constructs, this should replace the RDF Mapper tests in RML Engine

}
