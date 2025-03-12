package io.carml.vocab;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@SuppressWarnings("java:S115")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Csvw {

    public static final String PREFIX = "csvw";

    public static final String NAMESPACE = "http://www.w3.org/ns/csvw#";

    public static final String Table = NAMESPACE + "Table";

    public static final String url = NAMESPACE + "url";

    public static final String dialect = NAMESPACE + "dialect";

    public static final String notes = NAMESPACE + "notes";

    public static final String suppressOutput = NAMESPACE + "suppressOutput";

    public static final String tableDirection = NAMESPACE + "tableDirection";

    public static final String tableSchema = NAMESPACE + "tableSchema";

    public static final String transformations = NAMESPACE + "transformations";

    public static final String Dialect = NAMESPACE + "Dialect";

    public static final String columnSeparator = NAMESPACE + "columnSeparator";

    public static final String commentPrefix = NAMESPACE + "commentPrefix";

    public static final String encoding = NAMESPACE + "encoding";

    public static final String delimiter = NAMESPACE + "delimiter";

    public static final String header = NAMESPACE + "header";

    public static final String headerRowCount = NAMESPACE + "headerRowCount";

    public static final String skipRows = NAMESPACE + "skipRows";

    public static final String lineTerminators = NAMESPACE + "lineTerminators";

    public static final String quoteChar = NAMESPACE + "quoteChar";

    public static final String doubleQuote = NAMESPACE + "doubleQuote";

    public static final String skipColumns = NAMESPACE + "skipColumns";

    public static final String skipBlankRows = NAMESPACE + "skipBlankRows";

    public static final String skipInitialSpace = NAMESPACE + "skipInitialSpace";

    public static final String trim = NAMESPACE + "trim";
}
