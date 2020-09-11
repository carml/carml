package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.rmltestcases.model.Dataset;
import com.taxonic.carml.rmltestcases.model.Distribution;

public class RtcDataset extends RtcResource implements Dataset {

	private Distribution distribution;

	@RdfProperty("http://www.w3.org/ns/dcat#distribution")
	@RdfType(RtcDistribution.class)
	@Override
	public Distribution getDistribution() {
		return distribution;
	}

	public void setDistribution(Distribution distribution) {
		this.distribution = distribution;
	}

}
