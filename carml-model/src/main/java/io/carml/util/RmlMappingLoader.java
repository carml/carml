package io.carml.util;

import io.carml.model.FileSource;
import io.carml.model.NameableStream;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.model.XmlSource;
import io.carml.model.impl.CarmlFileSource;
import io.carml.model.impl.CarmlStream;
import io.carml.model.impl.CarmlTriplesMap;
import io.carml.model.impl.CarmlXmlSource;
import io.carml.rdfmapper.impl.MappingCache;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;

public class RmlMappingLoader {

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

    return Set.copyOf(RdfObjectLoader.load(RmlMappingLoader::selectTriplesMaps, CarmlTriplesMap.class, model,
        shorthandExpander, this::addTermTypes, mapper -> {
          mapper.addDecidableType(Rdf.Carml.Stream, NameableStream.class);
          mapper.addDecidableType(Rdf.Carml.XmlDocument, XmlSource.class);
          mapper.addDecidableType(Rdf.Carml.FileSource, FileSource.class);
          mapper.bindInterfaceImplementation(NameableStream.class, CarmlStream.class);
          mapper.bindInterfaceImplementation(XmlSource.class, CarmlXmlSource.class);
          mapper.bindInterfaceImplementation(FileSource.class, CarmlFileSource.class);
        }, RmlNamespaces.RML_NAMESPACES));
  }

  private static Set<Resource> selectTriplesMaps(Model model) {
    return Set.copyOf(model.filter(null, Rdf.Rml.logicalSource, null)
        .subjects());
  }

  private void addTermTypes(MappingCache cache) {
    class AddTermTypes {

      void add(IRI iri, TermType termType) {
        cache.addCachedMapping(iri, Set.of(TermType.class), termType);
      }

      void run() {
        add(Rdf.Rr.BlankNode, TermType.BLANK_NODE);
        add(Rdf.Rr.IRI, TermType.IRI);
        add(Rdf.Rr.Literal, TermType.LITERAL);
      }
    }

    new AddTermTypes().run();
  }
}
