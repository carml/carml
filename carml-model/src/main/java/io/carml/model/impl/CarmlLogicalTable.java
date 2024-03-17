package io.carml.model.impl;

import io.carml.model.LogicalTable;
import io.carml.model.Resource;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
public class CarmlLogicalTable extends CarmlLogicalSource implements LogicalTable {

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rr.LogicalTable);

        if (tableName != null) {
            modelBuilder.add(Rr.tableName, tableName);
        }
        if (sqlQuery != null) {
            modelBuilder.add(Rr.sqlQuery, sqlQuery);
        }
        if (sqlVersion != null) {
            modelBuilder.add(Rr.sqlVersion, sqlVersion);
        }
    }
}
