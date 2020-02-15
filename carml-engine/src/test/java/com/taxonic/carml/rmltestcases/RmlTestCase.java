package com.taxonic.carml.rmltestcases;

import java.util.Set;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.rmltestcases.model.Dataset;
import com.taxonic.carml.rmltestcases.model.Input;
import com.taxonic.carml.rmltestcases.model.Output;
import com.taxonic.carml.rmltestcases.model.Rules;
import com.taxonic.carml.rmltestcases.model.TestCase;

public class RmlTestCase extends RtcResource implements TestCase {

	private String identifier;
	private String description;
	private String specificationReference;
	private Set<Dataset> parts;
	private Rules rules;
	private Output output;

	@RdfProperty("http://purl.org/dc/terms/identifier")
	@Override
	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	@RdfProperty("http://purl.org/dc/terms/description")
	@Override
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String getSpecificationReference() {
		return specificationReference;
	}

	public void setSpecificationReference(String specificationReference) {
		this.specificationReference = specificationReference;
	}

	@Override
	public Set<Dataset> getParts() {
		return parts;
	}

	public void setParts(Set<Dataset> parts) {
		this.parts = parts;
	}

	@Override
	public Rules getRules() {
		return rules;
	}

	public void setRules(Rules rules) {
		this.rules = rules;
	}

	@Override
	public Set<Input> getInput() {
		return parts.stream() //
				.filter(p -> p.getId().contains("/input")) //
				.map(Input.class::cast) //
				.collect(ImmutableCollectors.toImmutableSet());
	}

	@Override
	public Output getOutput() {
		return output;
	}

	public void setOutput(Output output) {
		this.output = output;
	}

}
