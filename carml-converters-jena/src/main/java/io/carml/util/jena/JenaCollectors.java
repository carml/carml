package io.carml.util.jena;

import java.util.function.Supplier;
import java.util.stream.Collector;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

public class JenaCollectors {

  private JenaCollectors() {}

  /**
   * Returns a {@link Collector} that accumulates the input {@link Quad}s into a new
   * {@link DatasetGraph}.
   *
   * @return the {@link Collector}
   */
  public static Collector<Quad, DatasetGraph, DatasetGraph> toDatasetGraph() {
    return toDatasetGraph(DatasetGraphFactory::createTxnMem);
  }

  /**
   * Returns a {@link Collector} that accumulates the input {@link Quad}s into a new
   * {@link DatasetGraph}.
   *
   * @param datasetGraphSupplier The {@link DatasetGraph} {@link Supplier} function for the new
   *        collector.
   * @return the {@link Collector}
   */
  public static Collector<Quad, DatasetGraph, DatasetGraph> toDatasetGraph(
      Supplier<DatasetGraph> datasetGraphSupplier) {
    return Collector.of(datasetGraphSupplier, DatasetGraph::add, JenaCollectors::combineDatasetGraphs,
        Collector.Characteristics.UNORDERED);
  }

  public static Collector<Quad, DatasetGraph, Dataset> toDataset() {
    return toDataset(DatasetGraphFactory::createTxnMem);
  }

  /**
   * Returns a {@link Collector} that accumulates the input {@link Quad}s into a new {@link Dataset}.
   *
   * @param datasetGraphSupplier The {@link DatasetGraph} {@link Supplier} function for the new
   *        collector.
   * @return the {@link Collector}
   */
  public static Collector<Quad, DatasetGraph, Dataset> toDataset(Supplier<DatasetGraph> datasetGraphSupplier) {
    return Collector.of(datasetGraphSupplier, DatasetGraph::add, JenaCollectors::combineDatasetGraphs,
        DatasetFactory::wrap, Collector.Characteristics.UNORDERED);
  }

  private static DatasetGraph combineDatasetGraphs(DatasetGraph left, DatasetGraph right) {
    left.addAll(right);
    return left;
  }
}
