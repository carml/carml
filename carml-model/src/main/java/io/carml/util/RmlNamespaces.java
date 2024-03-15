package io.carml.util;

import com.google.common.collect.ImmutableSet;
import io.carml.vocab.Carml;
import io.carml.vocab.Fnml;
import io.carml.vocab.Fno;
import io.carml.vocab.OldRml;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class RmlNamespaces {

  public static final Set<Namespace> RML_NAMESPACES = new ImmutableSet.Builder<Namespace>().add(RDF.NS)
      .add(RDFS.NS)
      .add(XSD.NS)
      .add(new SimpleNamespace(Rml.PREFIX, Rml.NAMESPACE))
      .add(new SimpleNamespace(Rr.PREFIX, Rr.NAMESPACE))
      .add(new SimpleNamespace(OldRml.PREFIX, OldRml.NAMESPACE))
      .add(new SimpleNamespace(Fnml.PREFIX, Fnml.NAMESPACE))
      .add(new SimpleNamespace(Fno.PREFIX, Fno.NAMESPACE))
      .add(new SimpleNamespace(Carml.PREFIX, Carml.NAMESPACE))
      .add(new SimpleNamespace("ql", "http://semweb.mmlab.be/ns/ql#"))
      .build();

  private RmlNamespaces() {}

  public static Model applyRmlNameSpaces(Model model) {
    RML_NAMESPACES.forEach(model::setNamespace);
    return model;
  }
}
