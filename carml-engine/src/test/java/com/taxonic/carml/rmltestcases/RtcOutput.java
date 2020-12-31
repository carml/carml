package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.rmltestcases.model.Output;

public class RtcOutput extends RtcDataset implements Output {

  @Override
  public boolean isError() {
    return this.id.equals("http://rml.io/ns/test-case/InvalidRulesError");
  }

}
