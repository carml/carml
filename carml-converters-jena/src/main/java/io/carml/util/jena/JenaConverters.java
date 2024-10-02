package io.carml.util.jena;

import static org.apache.jena.graph.NodeFactory.createBlankNode;
import static org.apache.jena.graph.NodeFactory.createLiteralDT;
import static org.apache.jena.graph.NodeFactory.createLiteralLang;
import static org.apache.jena.graph.NodeFactory.createTripleNode;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.sparql.core.Quad.defaultGraphIRI;

import lombok.NonNull;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

public final class JenaConverters {

  private static final TypeMapper TYPE_MAPPER = TypeMapper.getInstance();

  private JenaConverters() {}

  /**
   * Transforms an RDF4J {@link Value} to an equivalent Apache Jena {@link Node}.
   *
   * @param value The {@link Value} to transform.
   * @return the {@link Node}.
   */
  public static Node toNode(@NonNull Value value) {
    if (value.isIRI()) {
      return createURI(value.stringValue());
    } else if (value.isBNode()) {
      return createBlankNode(((BNode) value).getID());
    } else if (value.isLiteral()) {
      var literalValue = (Literal) value;
      return literalValue.getLanguage()
          .map(lang -> createLiteralLang(literalValue.stringValue(), lang))
          .orElse(createLiteralDT(literalValue.stringValue(), TYPE_MAPPER.getSafeTypeByName(literalValue.getDatatype()
              .stringValue())));
    } else if (value.isTriple()) {
      var triple = (org.eclipse.rdf4j.model.Triple) value;
      return createTripleNode(toNode(triple.getSubject()), toNode(triple.getPredicate()), toNode(triple.getObject()));
    }

    throw new IllegalArgumentException(String.format("Unsupported value type %s", value.getClass()));
  }

  /**
   * Transforms an RDF4J {@link Statement} to an equivalent Apache Jena {@link Quad}.
   *
   * @param statement The {@link Statement}.
   * @return the {@link Quad}.
   */
  public static Quad toQuad(@NonNull Statement statement) {
    var context = statement.getContext();
    return Quad.create(context == null ? defaultGraphIRI : toNode(context), toNode(statement.getSubject()),
        toNode(statement.getPredicate()), toNode(statement.getObject()));
  }
}
