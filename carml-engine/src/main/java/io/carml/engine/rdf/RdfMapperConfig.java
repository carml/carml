package io.carml.engine.rdf;

import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
public class RdfMapperConfig {

  @NonNull
  private final Supplier<ValueFactory> valueFactorySupplier;

  @NonNull
  private final TermGeneratorFactory<Value> termGeneratorFactory;

  private final ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

  private final ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider;

}
