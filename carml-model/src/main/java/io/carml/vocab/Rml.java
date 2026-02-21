package io.carml.vocab;

@SuppressWarnings({"java:S115", "java:S1845"})
public class Rml {

    private Rml() {}

    public static final String PREFIX = "rml";

    public static final String NAMESPACE = "http://w3id.org/rml/";

    public static final String clazz = NAMESPACE + "class";

    public static final String language = NAMESPACE + "language";

    public static final String datatype = NAMESPACE + "datatype";

    public static final String template = NAMESPACE + "template";

    public static final String predicate = NAMESPACE + "predicate";

    public static final String predicateMap = NAMESPACE + "predicateMap";

    public static final String subject = NAMESPACE + "subject";

    public static final String subjectMap = NAMESPACE + "subjectMap";

    public static final String object = NAMESPACE + "object";

    public static final String objectMap = NAMESPACE + "objectMap";

    public static final String inverseExpression = NAMESPACE + "inverseExpression";

    public static final String termType = NAMESPACE + "termType";

    public static final String constant = NAMESPACE + "constant";

    public static final String predicateObjectMap = NAMESPACE + "predicateObjectMap";

    public static final String graphMap = NAMESPACE + "graphMap";

    public static final String TriplesMap = NAMESPACE + "TriplesMap";

    public static final String baseIRI = NAMESPACE + "baseIRI";

    public static final String SubjectMap = NAMESPACE + "SubjectMap";

    public static final String PredicateMap = NAMESPACE + "PredicateMap";

    public static final String ObjectMap = NAMESPACE + "ObjectMap";

    public static final String RefObjectMap = NAMESPACE + "RefObjectMap";

    public static final String PredicateObjectMap = NAMESPACE + "PredicateObjectMap";

    public static final String GraphMap = NAMESPACE + "GraphMap";

    public static final String parentTriplesMap = NAMESPACE + "parentTriplesMap";

    public static final String joinCondition = NAMESPACE + "joinCondition";

    public static final String Join = NAMESPACE + "Join";

    public static final String childMap = NAMESPACE + "childMap";

    public static final String child = NAMESPACE + "child";

    public static final String parentMap = NAMESPACE + "parentMap";

    public static final String parent = NAMESPACE + "parent";

    public static final String IRI = NAMESPACE + "IRI";

    public static final String URI = NAMESPACE + "URI";

    public static final String UnsafeIRI = NAMESPACE + "UnsafeIRI";

    public static final String BLANK_NODE = NAMESPACE + "BlankNode";

    public static final String LITERAL = NAMESPACE + "Literal";

    public static final String column = NAMESPACE + "column";

    public static final String logicalTable = NAMESPACE + "logicalTable";

    public static final String tableName = NAMESPACE + "tableName";

    public static final String sqlQuery = NAMESPACE + "sqlQuery";

    public static final String sqlVersion = NAMESPACE + "sqlVersion";

    public static final String SQL2008 = NAMESPACE + "SQL2008";

    public static final String Oracle = NAMESPACE + "Oracle";

    public static final String MySQL = NAMESPACE + "MySQL";

    public static final String MSSQLServer = NAMESPACE + "MSSQLServer";

    public static final String HSQLDB = NAMESPACE + "HSQLDB";

    public static final String PostgreSQL = NAMESPACE + "PostgreSQL";

    public static final String DB2 = NAMESPACE + "DB2";

    public static final String Informix = NAMESPACE + "Informix";

    public static final String Ingres = NAMESPACE + "Ingres";

    public static final String Progress = NAMESPACE + "Progress";

    public static final String SybaseASE = NAMESPACE + "SybaseASE";

    public static final String SybaseSQLAnywhere = NAMESPACE + "SybaseSQLAnywhere";

    public static final String Virtuoso = NAMESPACE + "Virtuoso";

    public static final String Firebird = NAMESPACE + "Firebird";

    public static final String BaseSource = NAMESPACE + "BaseSource";

    public static final String referenceFormulation = NAMESPACE + "referenceFormulation";

    public static final String source = NAMESPACE + "source";

    public static final String reference = NAMESPACE + "reference";

    public static final String iterator = NAMESPACE + "iterator";

    public static final String LogicalSource = NAMESPACE + "LogicalSource";

    public static final String logicalSource = NAMESPACE + "logicalSource";

    public static final String datatypeMap = NAMESPACE + "datatypeMap";

    public static final String languageMap = NAMESPACE + "languageMap";

    public static final String DatatypeMap = NAMESPACE + "DatatypeMap";

    public static final String LanguageMap = NAMESPACE + "LanguageMap";

    public static final String query = NAMESPACE + "query";

    public static final String root = NAMESPACE + "root";

    public static final String path = NAMESPACE + "path";

    public static final String gatherMap = NAMESPACE + "gatherMap";

    public static final String GatherMap = NAMESPACE + "GatherMap";

    public static final String strategy = NAMESPACE + "strategy";

    public static final String gatherAs = NAMESPACE + "gatherAs";

    public static final String gather = NAMESPACE + "gather";

    public static final String allowEmptyListAndContainer = NAMESPACE + "allowEmptyListAndContainer";

    public static final String logicalTarget = NAMESPACE + "logicalTarget";

    public static final String target = NAMESPACE + "target";

    public static final String serialization = NAMESPACE + "serialization";

    public static final String compression = NAMESPACE + "compression";

    public static final String none = NAMESPACE + "none";

    public static final String gzip = NAMESPACE + "gzip";

    public static final String zip = NAMESPACE + "zip";

    public static final String tarxz = NAMESPACE + "tarxz";

    public static final String targz = NAMESPACE + "targz";

    public static final String encoding = NAMESPACE + "encoding";

    public static final String NULL = NAMESPACE + "null";

    public static final String UTF_8 = NAMESPACE + "UTF-8";

    public static final String UTF_16 = NAMESPACE + "UTF-16";

    public static final String namespace = NAMESPACE + "namespace";

    public static final String namespacePrefix = NAMESPACE + "namespacePrefix";

    public static final String namespaceURL = NAMESPACE + "namespaceURL";

    // LogicalView

    public static final String LogicalView = NAMESPACE + "LogicalView";

    public static final String viewOn = NAMESPACE + "viewOn";

    public static final String field = NAMESPACE + "field";

    public static final String leftJoin = NAMESPACE + "leftJoin";

    public static final String innerJoin = NAMESPACE + "innerJoin";

    // Fields

    public static final String Field = NAMESPACE + "Field";

    public static final String ExpressionField = NAMESPACE + "ExpressionField";

    public static final String IterableField = NAMESPACE + "IterableField";

    public static final String fieldName = NAMESPACE + "fieldName";

    // LogicalViewJoin

    public static final String LogicalViewJoin = NAMESPACE + "LogicalViewJoin";

    public static final String parentLogicalView = NAMESPACE + "parentLogicalView";

    // Structural annotations

    public static final String structuralAnnotation = NAMESPACE + "structuralAnnotation";

    public static final String StructuralAnnotation = NAMESPACE + "StructuralAnnotation";

    public static final String IriSafeAnnotation = NAMESPACE + "IriSafeAnnotation";

    public static final String PrimaryKeyAnnotation = NAMESPACE + "PrimaryKeyAnnotation";

    public static final String UniqueAnnotation = NAMESPACE + "UniqueAnnotation";

    public static final String NotNullAnnotation = NAMESPACE + "NotNullAnnotation";

    public static final String ForeignKeyAnnotation = NAMESPACE + "ForeignKeyAnnotation";

    public static final String InclusionAnnotation = NAMESPACE + "InclusionAnnotation";

    public static final String onFields = NAMESPACE + "onFields";

    public static final String targetView = NAMESPACE + "targetView";

    public static final String targetFields = NAMESPACE + "targetFields";
}
