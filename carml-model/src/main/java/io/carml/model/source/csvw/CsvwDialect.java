package io.carml.model.source.csvw;

import io.carml.model.Resource;
import java.util.List;

public interface CsvwDialect extends Resource {

    String getCommentPrefix();

    String getDelimiter();

    String getDoubleQuote();

    String getEncoding();

    boolean hasHeader();

    int getHeaderRowCount();

    List<String> getLineTerminators();

    String getQuoteChar();

    boolean getSkipBlankRows();

    int getSkipColumns();

    boolean getSkipInitialSpace();

    int getSkipRows();

    boolean trim();
}
