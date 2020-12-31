package com.taxonic.carml.util;

import java.io.IOException;
import java.io.InputStream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;

public class IoUtils {

  private IoUtils() {}

  public static Model parseTrig(String resource) {
    return parse(resource, RDFFormat.TRIG);
  }

  public static Model parse(String resource) {
    return parse(resource, RDFFormat.TURTLE);
  }

  public static Model parse(String resource, RDFFormat format) {
    try (InputStream input = IoUtils.class.getClassLoader()
        .getResourceAsStream(resource)) {
      return parse(input, format);
    } catch (IOException e) {
      throw new RuntimeException("failed to parse resource [" + resource + "] as [" + format + "]", e);
    }
  }

  public static Model parse(InputStream inputStream, RDFFormat format) {
    try (InputStream is = inputStream) {
      ParserConfig settings = new ParserConfig();
      settings.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
      return Rio.parse(is, "http://none.com/", format, settings, SimpleValueFactory.getInstance(),
          new ParseErrorLogger());
    } catch (IOException e) {
      throw new RuntimeException("failed to parse input stream [" + inputStream + "] as [" + format + "]", e);
    }
  }
}
