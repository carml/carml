package io.carml.rmltestcases;

import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.rmltestcases.model.Dataset;
import io.carml.rmltestcases.model.Distribution;

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
