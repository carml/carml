package com.taxonic.carml.util;

import java.net.URISyntaxException;
import lombok.NonNull;
import org.eclipse.rdf4j.common.net.ParsedIRI;

public final class RdfValues {

  private RdfValues() {}

  public static boolean isValidIri(@NonNull String str) {
    if (!str.contains(":")) {
      return false;
    }
    try {
      return new ParsedIRI(str).getScheme() != null;
    } catch (URISyntaxException uriException) {
      return false;
    }
  }

}
