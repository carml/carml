package io.carml.model.source.csvw;

import java.util.List;

public interface CsvwTable {

    String getUrl();

    CsvwDialect getDialect();

    List<String> getNotes();

    boolean suppressOutput();

    CsvwDirection getTableDirection();

    CsvwSchema getTableSchema();

    List<CsvwTransformation> getTransformations();
}
