package io.carml.engine.rdf;

import io.carml.engine.function.Functions;
import java.text.Normalizer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
public class RdfMapperOptions {

  @NonNull
  private IRI baseIri;

  @NonNull
  ValueFactory valueFactory;

  @NonNull
  private final Normalizer.Form normalizationForm;

  private final boolean iriUpperCasePercentEncoding;

  private final Functions functions;

}
