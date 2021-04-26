package com.taxonic.carml.util;

import java.util.stream.Collector;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelCollector;

public final class RdfCollectors {

  private RdfCollectors() {}

  public static Collector<Statement, Model, Model> toRdf4JModel() {
    return ModelCollector.toModel();
  }

  public static Collector<Statement, Model, Model> toRdf4JTreeModel() {
    return ModelCollector.toTreeModel();
  }

  // TODO add JENA model collector
  // public static toJenaModel() {
  //
  // }
}
