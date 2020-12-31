package com.taxonic.carml.util;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.*;
import com.taxonic.carml.model.impl.CarmlFileSource;
import com.taxonic.carml.model.impl.CarmlStream;
import com.taxonic.carml.model.impl.CarmlTriplesMap;
import com.taxonic.carml.model.impl.CarmlXmlSource;
import com.taxonic.carml.rdf_mapper.impl.MappingCache;
import com.taxonic.carml.rdf_mapper.util.RdfObjectLoader;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rdf.Rr;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;

public class RmlMappingLoader {

  private RmlConstantShorthandExpander shorthandExpander;

  public RmlMappingLoader(RmlConstantShorthandExpander shorthandExpander) {
    this.shorthandExpander = shorthandExpander;
  }

  /**
   * @deprecated use {@link #load(RDFFormat, String...)} instead.
   */
  @Deprecated
  public Set<TriplesMap> load(String resource, RDFFormat rdfFormat) {
    return load(rdfFormat, resource);
  }

  /**
   * @deprecated use {@link #load(RDFFormat, Path...)} instead.
   */
  @Deprecated
  public Set<TriplesMap> load(Path pathToFile, RDFFormat rdfFormat) {
    return load(rdfFormat, pathToFile);
  }

  /**
   * @deprecated use {@link #load(RDFFormat, InputStream...)} instead.
   */
  @Deprecated
  public Set<TriplesMap> load(InputStream input, RDFFormat rdfFormat) {
    return load(rdfFormat, input);
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, String... classPathResources) {
    InputStream[] inputs = Arrays.stream(classPathResources)
        .map(r -> RmlMappingLoader.class.getClassLoader()
            .getResourceAsStream(r))
        .toArray(InputStream[]::new);

    return load(rdfFormat, inputs);
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, Path... pathsToFile) {
    InputStream[] inputs = Arrays.stream(pathsToFile) //
        .map(p -> {
          InputStream input;
          try {
            input = Files.newInputStream(p);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return input;
        })
        .toArray(InputStream[]::new);

    return load(rdfFormat, inputs);
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, InputStream... inputs) {
    Model[] models = Arrays.stream(inputs) //
        .map(i -> parse(i, rdfFormat)) //
        .toArray(Model[]::new);

    return load(models);
  }

  private Model parse(InputStream inputStream, RDFFormat format) {
    try (InputStream is = inputStream) {
      ParserConfig settings = new ParserConfig();
      settings.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
      return Rio.parse(is, "http://none.com/", format, settings, SimpleValueFactory.getInstance(),
          new ParseErrorLogger());
    } catch (IOException e) {
      throw new RuntimeException("failed to parse input stream [" + inputStream + "] as [" + format + "]", e);
    }
  }

  public Set<TriplesMap> load(Model... models) {
    Model model = Arrays.stream(models) //
        .flatMap(Collection::stream) //
        .collect(Collectors.toCollection(LinkedHashModel::new));

    return ImmutableSet.copyOf( //
        RdfObjectLoader.load( //
            selectTriplesMaps, //
            CarmlTriplesMap.class, //
            model, //
            shorthandExpander, //
            this::addTermTypes, //
            r -> { //
              r.addDecidableType(Rdf.Carml.Stream, NameableStream.class);
              r.addDecidableType(Rdf.Carml.XmlDocument, XmlSource.class);
              r.addDecidableType(Rdf.Carml.FileSource, FileSource.class);
              r.bindInterfaceImplementation(NameableStream.class, CarmlStream.class);
              r.bindInterfaceImplementation(XmlSource.class, CarmlXmlSource.class);
              r.bindInterfaceImplementation(FileSource.class, CarmlFileSource.class);
            }, RmlNamespaces.RML_NAMESPACES));
  }

  private void addTermTypes(MappingCache cache) {
    class AddTermTypes {

      void add(IRI iri, TermType termType) {
        cache.addCachedMapping(iri, ImmutableSet.of(TermType.class), termType);
      }

      void run() {
        add(Rr.BlankNode, TermType.BLANK_NODE);
        add(Rr.IRI, TermType.IRI);
        add(Rr.Literal, TermType.LITERAL);
      }
    }
    new AddTermTypes().run();
  }

  public static RmlMappingLoader build() {
    return new RmlMappingLoader(new RmlConstantShorthandExpander());
  }

  private static Function<Model, Set<Resource>> selectTriplesMaps =
      model -> ImmutableSet.copyOf(model.filter(null, Rdf.Rml.logicalSource, null)
          .subjects());
}
