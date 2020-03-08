package com.taxonic.carml.rdf_mapper.impl;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;

public class Person {

	private String name;
	private PostalAddress address;
	private Set<Person> knows;

	static final String SCHEMAORG = "http://schema.org/";
	static final String SCHEMAORG_NAME = SCHEMAORG + "name";
	static final String SCHEMAORG_ADDRESS = SCHEMAORG + "address";
	static final String SCHEMAORG_KNOWS = SCHEMAORG + "knows";
	static final String SCHEMAORG_RELATED_TO = SCHEMAORG + "relatedTo";
	static final String SCHEMAORG_COLLEAGUES = SCHEMAORG + "colleagues";

	@RdfProperty(SCHEMAORG_NAME)
	public Optional<String> getName() {
		return Optional.ofNullable(name);
	}

	public void setName(String name) {
		this.name = name;
	}

	@RdfProperty(SCHEMAORG_ADDRESS)
	@RdfType(PostalAddress.class)
	public PostalAddress getAddress() {
		return address;
	}

	public void setAddress(PostalAddress address) {
		this.address = address;
	}

	@RdfProperty(SCHEMAORG_KNOWS)
	@RdfProperty(SCHEMAORG_RELATED_TO)
	@RdfProperty(value = SCHEMAORG_COLLEAGUES, deprecated = true)
	@RdfType(Person.class)
	public Set<Person> getKnows() {
		return knows;
	}

	public void setKnows(Set<Person> knows) {
		if (this.knows == null) {
			this.knows = new HashSet<>();
		}
		this.knows.addAll(knows);
	}

	@Override
	public String toString() {
		return "Person [name=" + name + ", address=" + address + ", knows=" + knows + "]";
	}

}
