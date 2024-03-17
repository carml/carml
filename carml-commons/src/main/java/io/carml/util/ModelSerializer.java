package io.carml.util;

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import lombok.NonNull;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

public final class ModelSerializer {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Pattern TTL_STYLE_PREFIX = Pattern.compile("[ \\t]*@prefix .*>\\s*.\\s*", Pattern.MULTILINE);

    private static final String NS = "http://ModelSerializer.carml.net/";

    private static final IRI CAUSED_EXCEPTION = VF.createIRI(String.format("%scausedException", NS));

    private static final Literal EXCEPTION_INDICATOR = VF.createLiteral("<<-<<-<<-<<-<<");

    public static final UnaryOperator<WriterConfig> SIMPLE_WRITER_CONFIG = wc -> {
        wc.set(BasicWriterSettings.PRETTY_PRINT, Boolean.TRUE);
        wc.set(BasicWriterSettings.INLINE_BLANK_NODES, Boolean.TRUE);
        return wc;
    };

    private ModelSerializer() {}

    public static String serializeAsRdf(
            @NonNull Model model, @NonNull RDFFormat rdfFormat, @NonNull UnaryOperator<Model> namespaceApplier) {
        return serializeAsRdf(model, rdfFormat, SIMPLE_WRITER_CONFIG, namespaceApplier);
    }

    public static String serializeAsRdf(
            @NonNull Model model,
            @NonNull RDFFormat rdfFormat,
            @NonNull UnaryOperator<WriterConfig> writerSettingsApplier,
            @NonNull UnaryOperator<Model> namespaceApplier) {
        namespaceApplier.apply(model);
        return serializeAsRdf(model, rdfFormat, writerSettingsApplier.apply(new WriterConfig()));
    }

    public static String serializeAsRdf(@NonNull Model model, @NonNull RDFFormat rdfFormat) {
        return serializeAsRdf(model, rdfFormat, new WriterConfig());
    }

    public static String serializeAsRdf(
            @NonNull Model model, @NonNull RDFFormat rdfFormat, @NonNull WriterConfig config) {
        var sw = new StringWriter();
        var writer = new BufferedWriter(sw);
        Rio.write(model, writer, rdfFormat, config);
        return sw.toString();
    }

    public static String formatResourceForLog(
            @NonNull Model contextModel,
            @NonNull Resource resource,
            Set<Namespace> namespaces,
            boolean causedException) {
        return formatResourceForLog(contextModel, resource, new LinkedHashModel(), namespaces, causedException);
    }

    public static String formatResourceForLog(
            @NonNull Model contextModel,
            @NonNull Resource resource,
            @NonNull Model resourceModel,
            Set<Namespace> namespaces,
            boolean causedException) {
        if (resource instanceof IRI) {
            return String.format("resource <%s>", resource.stringValue());
        }

        Model reverseBNodeDescription = new LinkedHashModel();
        if (causedException) {
            reverseBNodeDescription.setNamespace("", NS);
            reverseBNodeDescription.add(resource, CAUSED_EXCEPTION, EXCEPTION_INDICATOR);
        }
        reverseBNodeDescription.addAll(Models.symmetricDescribeResource(contextModel, resource));
        reverseBNodeDescription.addAll(resourceModel);

        String ttl = ModelSerializer.serializeAsRdf(reverseBNodeDescription, RDFFormat.TURTLE, mdl -> {
            namespaces.forEach(mdl::setNamespace);
            return contextModel;
        });
        return String.format(
                "blank node resource %s in:%n```%n%s%n```",
                resource, stripTurtleStylePrefixes(ttl).trim());
    }

    public static String stripTurtleStylePrefixes(@NonNull String ttlStyleSerialization) {
        return TTL_STYLE_PREFIX.matcher(ttlStyleSerialization).replaceAll("");
    }
}
