package io.carml.model.impl;

import io.carml.rdfmapper.Mapper;
import io.carml.rdfmapper.TypeDecider;
import io.carml.vocab.Rdf.Carml;
import io.carml.vocab.Rdf.D2rq;
import io.carml.vocab.Rdf.Rml;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class LogicalSourceSourceTypeDecider implements TypeDecider {

    private final Mapper mapper;

    public LogicalSourceSourceTypeDecider(Mapper mapper) {
        this.mapper = mapper;
    }

    private static final Map<IRI, IRI> INFERENCE_MAP = Map.of(
            Carml.streamName,
            Carml.Stream, //
            Carml.declaresNamespace,
            Carml.XmlDocument, //
            Carml.url,
            Carml.FileSource, //
            D2rq.jdbcDriver,
            D2rq.Database, //
            Rml.path,
            Rml.RelativePathSource);

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        Set<IRI> rdfTypes = model.filter(resource, RDF.TYPE, null).objects().stream()
                .map(IRI.class::cast)
                .collect(Collectors.toSet());

        Set<IRI> usedPredicates = model.filter(resource, null, null).predicates().stream()
                .filter(p -> !p.equals(RDF.TYPE))
                .collect(Collectors.toSet());

        usedPredicates.forEach(p -> {
            if (INFERENCE_MAP.containsKey(p)) {
                rdfTypes.add(INFERENCE_MAP.get(p));
            }
        });

        return rdfTypes.stream().map(mapper::getDecidableType).collect(Collectors.toSet());
    }
}
