package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rmltestcases.model.Distribution;
import com.taxonic.carml.rmltestcases.model.Output;

public class RtcOutput extends RtcResource implements Output {

	private Distribution distribution;

	@RdfProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result")
	@Override
	public Distribution getDistribution() {
		return distribution;
	}

	public void setDistribution(Distribution distribution) {
		this.distribution = distribution;
	}

	@Override
	public boolean isError() {
		return this.id.equals("http://rml.io/ns/test-case/InvalidRulesError");
	}

}
