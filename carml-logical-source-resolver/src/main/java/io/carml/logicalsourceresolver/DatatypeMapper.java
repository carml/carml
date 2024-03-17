package io.carml.logicalsourceresolver;

import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;

public interface DatatypeMapper extends Function<String, Optional<IRI>> {}
