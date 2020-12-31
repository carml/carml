package com.taxonic.carml.model;

import com.taxonic.carml.rdf_mapper.Combiner;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

public class AsRdfCombiner implements Combiner<Model> {

  @Override
  public Model combine(List<Model> delegateInvocationResults) {
    return delegateInvocationResults.stream()
        .flatMap(Model::stream)
        .collect(Collectors.toCollection(LinkedHashModel::new));
  }
}
