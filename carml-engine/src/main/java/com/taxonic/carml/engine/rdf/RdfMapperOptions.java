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

  private final Normalizer.Form normalizationForm;

  private final boolean iriUpperCasePercentEncoding;

  private final Functions functions;

}
