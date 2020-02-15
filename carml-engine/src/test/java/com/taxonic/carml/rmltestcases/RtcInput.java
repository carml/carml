package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rmltestcases.model.Distribution;
import com.taxonic.carml.rmltestcases.model.Input;

public class RtcInput extends RtcResource implements Input {

	private Distribution distribution;

	@RdfProperty("http://rml.io/ns/test-case/rules")
	@Override
	public Distribution getDistribution() {
		return distribution;
	}

	public void setDistribution(Distribution distribution) {
		this.distribution = distribution;
	}

}
