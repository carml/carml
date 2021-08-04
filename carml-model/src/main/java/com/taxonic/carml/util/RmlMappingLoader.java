package com.taxonic.carml.util;

import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.XmlSource;
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
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;

public class RmlMappingLoader {

  private static final Function<Model, Set<Resource>> selectTriplesMaps =
      model -> Set.copyOf(model.filter(null, Rdf.Rml.logicalSource, null)
          .subjects());

  public static RmlMappingLoader build() {
    return new RmlMappingLoader(new RmlConstantShorthandExpander());
  }

  private final RmlConstantShorthandExpander shorthandExpander;

  private RmlMappingLoader(RmlConstantShorthandExpander shorthandExpander) {
    this.shorthandExpander = shorthandExpander;
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, String... classPathResources) {
    InputStream[] inputs = Arrays.stream(classPathResources)
        .map(RmlMappingLoader.class.getClassLoader()::getResourceAsStream)
        .toArray(InputStream[]::new);

    return load(rdfFormat, inputs);
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, Path... pathsToFile) {
    InputStream[] inputs = Arrays.stream(pathsToFile)
        .map(path -> {
          InputStream input;
          try {
            input = Files.newInputStream(path);
          } catch (IOException ioException) {
            throw new RmlMappingLoaderException(String.format("Exception while load mapping from path %s", path),
                ioException);
          }
          return input;
        })
        .toArray(InputStream[]::new);

    return load(rdfFormat, inputs);
  }

  public Set<TriplesMap> load(RDFFormat rdfFormat, InputStream... inputs) {
    Model[] models = Arrays.stream(inputs)
        .map(inputStream -> Models.parse(inputStream, rdfFormat))
        .toArray(Model[]::new);

    return load(models);
  }

  public Set<TriplesMap> load(Model... models) {
    Model model = Arrays.stream(models)
        .flatMap(Collection::stream)
        .collect(ModelCollector.toModel());

    return Set.copyOf(RdfObjectLoader.load(selectTriplesMaps, CarmlTriplesMap.class, model, shorthandExpander,
        this::addTermTypes, mapper -> {
          mapper.addDecidableType(Rdf.Carml.Stream, NameableStream.class);
          mapper.addDecidableType(Rdf.Carml.XmlDocument, XmlSource.class);
          mapper.addDecidableType(Rdf.Carml.FileSource, FileSource.class);
          mapper.bindInterfaceImplementation(NameableStream.class, CarmlStream.class);
          mapper.bindInterfaceImplementation(XmlSource.class, CarmlXmlSource.class);
          mapper.bindInterfaceImplementation(FileSource.class, CarmlFileSource.class);
        }, RmlNamespaces.RML_NAMESPACES));
  }

  private void addTermTypes(MappingCache cache) {
    class AddTermTypes {

      void add(IRI iri, TermType termType) {
        cache.addCachedMapping(iri, Set.of(TermType.class), termType);
      }

      void run() {
        add(Rr.BlankNode, TermType.BLANK_NODE);
        add(Rr.IRI, TermType.IRI);
        add(Rr.Literal, TermType.LITERAL);
      }
    }

    new AddTermTypes().run();
  }
}
