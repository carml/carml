package io.carml.engine.rdf;

import io.carml.engine.TermGenerator;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.GatherMap;
import java.util.List;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.util.RDFContainers;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@AllArgsConstructor(staticName = "of")
public class RdfCollectionOrContainerGenerator
        implements BiFunction<ExpressionEvaluation, DatatypeMapper, List<RdfCollectionOrContainer>> {

    private GatherMap gatherMap;

    private ValueFactory valueFactory;

    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    private TermGenerator<? extends Value> headGenerator;

    private List<Resource> contexts;

    @Override
    public List<RdfCollectionOrContainer> apply(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        var head = (Resource) headGenerator.apply(expressionEvaluation, datatypeMapper);

        return gatherMap.getGatheredOnes().stream()
                .map(termMap -> rdfTermGeneratorFactory.getObjectGenerator(termMap))
                .map(termGenerator -> termGenerator.apply(expressionEvaluation, datatypeMapper))
                .map(gatheredTerms -> createCollection(head, gatheredTerms))
                .toList();
    }

    private RdfCollectionOrContainer createCollection(Resource head, List<? extends Value> gatheredTerms) {
        var model = new LinkedHashModel();
        if (gatherMap.getGatherAs().equals(RDF.LIST)) {
            RDFCollections.asRDF(gatheredTerms, head, model, valueFactory, contexts.toArray(Resource[]::new));

            return new RdfCollectionOrContainer(RDF.LIST, head, model);
        }

        RDFContainers.toRDF(
                gatherMap.getGatherAs(), gatheredTerms, head, model, valueFactory, contexts.toArray(Resource[]::new));

        return new RdfCollectionOrContainer(gatherMap.getGatherAs(), head, model);
    }
}
