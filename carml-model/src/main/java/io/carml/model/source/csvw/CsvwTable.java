package io.carml.model.source.csvw;

import io.carml.model.Source;
import java.util.List;

public interface CsvwTable extends Source {

    String getUrl();

    CsvwDialect getDialect();

    List<String> getNotes();

    boolean suppressOutput();

    CsvwDirection getTableDirection();

    CsvwSchema getTableSchema();

    List<CsvwTransformation> getTransformations();
}
