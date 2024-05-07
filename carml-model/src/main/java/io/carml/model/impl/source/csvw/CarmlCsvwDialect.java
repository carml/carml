package io.carml.model.impl.source.csvw;

import io.carml.model.Resource;
import io.carml.model.impl.CarmlResource;
import io.carml.model.source.csvw.CsvwDialect;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Csvw;
import io.carml.vocab.Rdf;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlCsvwDialect extends CarmlResource implements CsvwDialect {

    private String commentPrefix;

    private String delimiter;

    private String doubleQuote;

    private String encoding;

    private boolean hasHeader;

    private int headerRowCount;

    @Singular
    private List<String> lineTerminators;

    private String quoteChar;

    private boolean skipBlankRows;

    private int skipColumns;

    private boolean skipInitialSpace;

    private int skipRows;

    private boolean trim;

    @RdfProperty(Csvw.commentPrefix)
    @Override
    public String getCommentPrefix() {
        return commentPrefix;
    }

    @RdfProperty(Csvw.delimiter)
    @Override
    public String getDelimiter() {
        return delimiter;
    }

    @RdfProperty(Csvw.doubleQuote)
    @Override
    public String getDoubleQuote() {
        return doubleQuote;
    }

    @RdfProperty(Csvw.encoding)
    @Override
    public String getEncoding() {
        return encoding;
    }

    @RdfProperty(Csvw.header)
    @Override
    public boolean hasHeader() {
        return hasHeader;
    }

    @RdfProperty(Csvw.headerRowCount)
    @Override
    public int getHeaderRowCount() {
        return headerRowCount;
    }

    @RdfProperty(Csvw.lineTerminators)
    @Override
    public List<String> getLineTerminators() {
        return lineTerminators;
    }

    @RdfProperty(Csvw.quoteChar)
    @Override
    public String getQuoteChar() {
        return quoteChar;
    }

    @RdfProperty(Csvw.skipBlankRows)
    @Override
    public boolean getSkipBlankRows() {
        return skipBlankRows;
    }

    @RdfProperty(Csvw.skipColumns)
    @Override
    public int getSkipColumns() {
        return skipColumns;
    }

    @RdfProperty(Csvw.skipInitialSpace)
    @Override
    public boolean getSkipInitialSpace() {
        return skipInitialSpace;
    }

    @RdfProperty(Csvw.skipRows)
    @Override
    public int getSkipRows() {
        return skipRows;
    }

    @RdfProperty(Csvw.trim)
    @Override
    public boolean trim() {
        return trim;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Csvw.Dialect);

        if (commentPrefix != null) {
            modelBuilder.add(Csvw.commentPrefix, commentPrefix);
        }
        if (delimiter != null) {
            modelBuilder.add(Csvw.delimiter, delimiter);
        }
        if (doubleQuote != null) {
            modelBuilder.add(Csvw.doubleQuote, doubleQuote);
        }
        if (encoding != null) {
            modelBuilder.add(Csvw.encoding, encoding);
        }
        modelBuilder.add(Csvw.header, hasHeader);
        modelBuilder.add(Csvw.headerRowCount, headerRowCount);
        lineTerminators.forEach(lineTerminator -> modelBuilder.add(Csvw.lineTerminators, lineTerminator));
        if (quoteChar != null) {
            modelBuilder.add(Csvw.quoteChar, quoteChar);
        }
        modelBuilder.add(Csvw.skipBlankRows, skipBlankRows);
        modelBuilder.add(Csvw.skipColumns, skipColumns);
        modelBuilder.add(Csvw.skipInitialSpace, skipInitialSpace);
        modelBuilder.add(Csvw.skipRows, skipRows);
        modelBuilder.add(Csvw.trim, trim);
    }
}
