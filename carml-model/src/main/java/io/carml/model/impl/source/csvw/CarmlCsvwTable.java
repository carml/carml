package io.carml.model.impl.source.csvw;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Resource;
import io.carml.model.impl.CarmlSource;
import io.carml.model.source.csvw.CsvwDialect;
import io.carml.model.source.csvw.CsvwDirection;
import io.carml.model.source.csvw.CsvwSchema;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.model.source.csvw.CsvwTransformation;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Csvw;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.Builder.Default;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@Slf4j
public class CarmlCsvwTable extends CarmlSource implements CsvwTable {

    private String url;

    private CsvwDialect dialect;

    @Singular
    private List<String> notes;

    private boolean suppressOutput;

    @Default
    private CsvwDirection tableDirection = CsvwDirection.AUTO;

    private CsvwSchema tableSchema;

    @Singular
    private List<CsvwTransformation> transformations;

    @RdfProperty(Csvw.url)
    @Override
    public String getUrl() {
        return url;
    }

    @RdfProperty(Csvw.dialect)
    @RdfType(CarmlCsvwDialect.class)
    @Override
    public CsvwDialect getDialect() {
        return dialect;
    }

    @RdfProperty(Csvw.notes)
    @Override
    public List<String> getNotes() {
        return notes;
    }

    @RdfProperty(Csvw.suppressOutput)
    @Override
    public boolean suppressOutput() {
        return suppressOutput;
    }

    @RdfProperty(Csvw.tableDirection)
    @Override
    public CsvwDirection getTableDirection() {
        return tableDirection;
    }

    @RdfProperty(Csvw.tableSchema)
    @Override
    public CsvwSchema getTableSchema() {
        return tableSchema;
    }

    @RdfProperty(Csvw.transformations)
    @Override
    public List<CsvwTransformation> getTransformations() {
        return transformations;
    }

    @RdfProperty(Rml.encoding)
    @Override
    public IRI getEncoding() {
        if (super.getEncoding() != null) {
            return super.getEncoding();
        }

        if (dialect.getEncoding() != null) {
            try {
                Charset.forName(dialect.getEncoding());
            } catch (UnsupportedCharsetException unsupportedCharsetException) {
                LOG.warn(
                        "Unsupported charset '{}'. Falling back to default charset '{}'.",
                        dialect.getEncoding(),
                        Charset.defaultCharset());
            }
        }

        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), url, dialect, notes, suppressOutput, tableDirection, tableSchema, transformations);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CsvwTable other) {
            return super.equalsSource(other)
                    && Objects.equals(url, other.getUrl())
                    && Objects.equals(dialect, other.getDialect())
                    && Objects.equals(notes, other.getNotes())
                    && suppressOutput == other.suppressOutput()
                    && tableDirection == other.getTableDirection()
                    && Objects.equals(tableSchema, other.getTableSchema())
                    && Objects.equals(transformations, other.getTransformations());
        }
        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();
        if (dialect != null) {
            builder.add(dialect);
        }
        if (tableSchema != null) {
            builder.add(tableSchema);
        }
        transformations.forEach(builder::add);

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Csvw.Table);

        if (url != null) {
            modelBuilder.add(Csvw.url, url);
        }
        if (dialect != null) {
            modelBuilder.add(Csvw.dialect, dialect.getAsResource());
        }
        notes.forEach(note -> modelBuilder.add(Csvw.notes, note));
        modelBuilder.add(Csvw.suppressOutput, suppressOutput);
        if (tableDirection != null) {
            addTableDirectionTriple(modelBuilder);
        }
        if (tableSchema != null) {
            modelBuilder.add(Csvw.tableSchema, tableSchema.getAsResource());
        }
        transformations.forEach(
                transformation -> modelBuilder.add(Csvw.transformations, transformation.getAsResource()));
    }

    private void addTableDirectionTriple(ModelBuilder builder) {
        switch (tableDirection) {
            case LTR:
                builder.add(Rdf.Rml.termType, Rdf.Csvw.ltr);
                break;
            case RTL:
                builder.add(Rdf.Rml.termType, Rdf.Csvw.rtl);
                break;
            case AUTO:
                builder.add(Rdf.Rml.termType, Rdf.Csvw.auto);
                break;
            default:
                throw new IllegalStateException(
                        String.format("Illegal table direction value '%s' encountered.", tableDirection));
        }
    }
}
