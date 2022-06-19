package io.carml.model;

import io.carml.rdfmapper.Combiner;
import java.util.List;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelCollector;

public class AsRdfCombiner implements Combiner<Model> {

  @Override
  public Model combine(List<Model> delegateInvocationResults) {
    return delegateInvocationResults.stream()
        .flatMap(Model::stream)
        .collect(ModelCollector.toModel());
  }
}
