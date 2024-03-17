package io.carml.util;

import io.carml.model.DatabaseSource;
import io.carml.model.FileSource;
import io.carml.model.NameableStream;
import io.carml.model.RelativePathSource;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.model.XmlSource;
import io.carml.model.impl.CarmlDatabaseSource;
import io.carml.model.impl.CarmlFileSource;
import io.carml.model.impl.CarmlRelativePathSource;
import io.carml.model.impl.CarmlStream;
import io.carml.model.impl.CarmlTriplesMap;
import io.carml.model.impl.CarmlXmlSource;
import io.carml.rdfmapper.impl.MappingCache;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rdf.OldRml;
import io.carml.vocab.Rdf.Rml;
import io.carml.vocab.Rdf.Rr;
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

    private static final Set<IRI> BASE_SOURCES = Set.of(Rml.logicalSource, OldRml.logicalSource, Rr.logicalTable);

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
                        throw new RmlMappingLoaderException(
                                String.format("Exception while load mapping from path %s", path), ioException);
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
        Model model = Arrays.stream(models).flatMap(Collection::stream).collect(ModelCollector.toModel());

        return Set.copyOf(RdfObjectLoader.load(
                RmlMappingLoader::selectTriplesMaps,
                CarmlTriplesMap.class,
                model,
                shorthandExpander,
                this::addTermTypes,
                mapper -> mapper.addDecidableType(Rdf.Carml.Stream, NameableStream.class)
                        .bindInterfaceImplementation(NameableStream.class, CarmlStream.class)
                        .addDecidableType(Rdf.Carml.XmlDocument, XmlSource.class)
                        .bindInterfaceImplementation(XmlSource.class, CarmlXmlSource.class)
                        .addDecidableType(Rdf.Carml.FileSource, FileSource.class)
                        .bindInterfaceImplementation(FileSource.class, CarmlFileSource.class)
                        .addDecidableType(Rdf.D2rq.Database, DatabaseSource.class)
                        .bindInterfaceImplementation(DatabaseSource.class, CarmlDatabaseSource.class)
                        .addDecidableType(Rdf.Rml.RelativePathSource, RelativePathSource.class)
                        .bindInterfaceImplementation(RelativePathSource.class, CarmlRelativePathSource.class),
                RmlNamespaces.RML_NAMESPACES));
    }

    private static Set<Resource> selectTriplesMaps(Model model) {
        return Set.copyOf(model.stream()
                .filter(statement -> BASE_SOURCES.contains(statement.getPredicate()))
                .collect(new ModelCollector())
                .subjects());
    }

    private void addTermTypes(MappingCache cache) {
        class AddTermTypes {

            void add(IRI iri, TermType termType) {
                cache.addCachedMapping(iri, Set.of(TermType.class), termType);
            }

            void run() {
                add(Rml.BlankNode, TermType.BLANK_NODE);
                add(Rr.BlankNode, TermType.BLANK_NODE);
                add(Rml.IRI, TermType.IRI);
                add(Rr.IRI, TermType.IRI);
                add(Rml.Literal, TermType.LITERAL);
                add(Rr.Literal, TermType.LITERAL);
            }
        }

        new AddTermTypes().run();
    }
}
