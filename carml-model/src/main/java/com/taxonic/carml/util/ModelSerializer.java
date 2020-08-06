package com.taxonic.carml.util;

import com.taxonic.carml.vocab.*;
import java.io.BufferedWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

public class ModelSerializer {

    private ModelSerializer() {}

    public static UnaryOperator<WriterConfig> SIMPLE_WRITER_CONFIG = wc -> {
        wc.set(BasicWriterSettings.PRETTY_PRINT, Boolean.TRUE);
        wc.set(BasicWriterSettings.INLINE_BLANK_NODES, Boolean.TRUE);
        return wc;
    };

    public static String serializeAsRdf(Model model, RDFFormat rdfFormat, UnaryOperator<Model>... namespaceAppliers) {
        return serializeAsRdf(model, rdfFormat, SIMPLE_WRITER_CONFIG, namespaceAppliers);
    }

    public static String serializeAsRdf(Model model, RDFFormat rdfFormat, UnaryOperator<WriterConfig> writerSettingsApplier,
                                       UnaryOperator<Model>... namespaceAppliers) {

        Arrays.stream(namespaceAppliers).forEach(operator -> operator.apply(model));
        return serializeAsRdf(model, rdfFormat, writerSettingsApplier.apply(new WriterConfig()));
    }

    public static String serializeAsRdf(Model model, RDFFormat rdfFormat, WriterConfig config) {
        model = applyRmlNameSpaces(model);
        StringWriter sw = new StringWriter();
        BufferedWriter writer = new BufferedWriter(sw);
        Rio.write(model, writer, rdfFormat, config);
        return sw.toString();
    }

    public static Model applyRmlNameSpaces(Model model) {
        model.setNamespace(RDF.NS);
        model.setNamespace(RDFS.NS);
        model.setNamespace(Rr.PREFIX, Rr.NAMESPACE);
        model.setNamespace(Rml.PREFIX, Rml.NAMESPACE);
        model.setNamespace(Fnml.PREFIX, Fnml.NAMESPACE);
        model.setNamespace(Fno.PREFIX, Fno.NAMESPACE);
        model.setNamespace(Carml.PREFIX, Carml.NAMESPACE);
        model.setNamespace("ql", "http://semweb.mmlab.be/ns/ql#");

        return model;
    }
}
