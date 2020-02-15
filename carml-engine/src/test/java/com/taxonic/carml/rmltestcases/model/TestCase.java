package com.taxonic.carml.rmltestcases.model;

import java.util.Set;

public interface TestCase extends Resource {

	String getIdentifier();

	String getDescription();

	String getSpecificationReference();

	Set<Dataset> getParts();

	Rules getRules();

	Set<Input> getInput();

	Output getOutput();

}
