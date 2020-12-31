package com.taxonic.carml.engine.rdf;

import com.taxonic.carml.engine.TermGeneratorFactory;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
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
public class RdfMappingContext {

  @NonNull
  private final Supplier<ValueFactory> valueFactorySupplier;

  @NonNull
  private final TermGeneratorFactory<Value> termGeneratorFactory;

  private final ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

}
