package io.carml.model.source.csvw;

import java.util.List;

public interface CsvwDialect {

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
