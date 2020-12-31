package com.taxonic.carml.engine.rdf;

import com.taxonic.carml.engine.function.Functions;
import java.text.Normalizer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
public class RdfMapperOptions {

  private Normalizer.Form normalizationForm;

  private boolean iriUpperCasePercentEncoding;

  private Functions functions;

}
