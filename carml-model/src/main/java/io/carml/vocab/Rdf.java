package io.carml.vocab;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

@SuppressWarnings({"java:S115", "java:S1845"})
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Rdf {

    private static final ValueFactory f = SimpleValueFactory.getInstance();

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Rml {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Rml.NAMESPACE + suffix);
        }

        public static final IRI logicalSource = iri("logicalSource");

        public static final IRI datatypeMap = iri("datatypeMap");

        public static final IRI languageMap = iri("languageMap");

        public static final IRI LogicalSource = iri("LogicalSource");

        public static final IRI source = iri("source");

        public static final IRI Source = iri("Source");

        public static final IRI iterator = iri("iterator");

        public static final IRI referenceFormulation = iri("referenceFormulation");

        public static final IRI JsonPath = iri("JSONPath");

        public static final IRI XPath = iri("XPath");

        public static final IRI XPathReferenceFormulation = iri("XPathReferenceFormulation");

        public static final IRI namespace = iri("namespace");

        public static final IRI Namespace = iri("Namespace");

        public static final IRI namespacePrefix = iri("namespacePrefix");

        public static final IRI namespaceURL = iri("namespaceURL");

        public static final IRI Csv = iri("CSV");

        public static final IRI Rdb = iri("Rdb");

        public static final IRI SQL2008Table = iri("SQL2008Table");

        public static final IRI SQL2008Query = iri("SQL2008Query");

        public static final IRI subject = iri("subject");

        public static final IRI subjectMap = iri("subjectMap");

        public static final IRI predicateObjectMap = iri("predicateObjectMap");

        public static final IRI predicate = iri("predicate");

        public static final IRI predicateMap = iri("predicateMap");

        public static final IRI object = iri("object");

        public static final IRI objectMap = iri("objectMap");

        public static final IRI graph = iri("graph");

        public static final IRI graphMap = iri("graphMap");

        public static final IRI clazz = iri("class");

        public static final IRI constant = iri("constant");

        public static final IRI reference = iri("reference");

        public static final IRI template = iri("template");

        public static final IRI defaultGraph = iri("defaultGraph");

        public static final IRI parentTriplesMap = iri("parentTriplesMap");

        public static final IRI joinCondition = iri("joinCondition");

        public static final IRI datatype = iri("datatype");

        public static final IRI language = iri("language");

        public static final IRI child = iri("child");

        public static final IRI childMap = iri("childMap");

        public static final IRI parent = iri("parent");

        public static final IRI parentMap = iri("parentMap");

        public static final IRI TriplesMap = iri("TriplesMap");

        public static final IRI SubjectMap = iri("SubjectMap");

        public static final IRI PredicateMap = iri("PredicateMap");

        public static final IRI ObjectMap = iri("ObjectMap");

        public static final IRI PredicateObjectMap = iri("PredicateObjectMap");

        public static final IRI RefObjectMap = iri("RefObjectMap");

        public static final IRI GraphMap = iri("GraphMap");

        public static final IRI Join = iri("Join");

        public static final IRI DatatypeMap = iri("DatatypeMap");

        public static final IRI LanguageMap = iri("LanguageMap");

        public static final IRI ChildMap = iri("ChildMap");

        public static final IRI ParentMap = iri("ParentMap");

        public static final IRI termType = iri("termType");

        public static final IRI Literal = iri("Literal");

        public static final IRI BlankNode = iri("BlankNode");

        public static final IRI IRI = iri("IRI");

        public static final IRI inverseExpression = iri("inverseExpression");

        public static final IRI SQL2008 = iri("SQL2008");

        public static final IRI Oracle = iri("Oracle");

        public static final IRI MySQL = iri("MySQL");

        public static final IRI MSSQLServer = iri("MSSQLServer");

        public static final IRI HSQLDB = iri("HSQLDB");

        public static final IRI PostgreSQL = iri("PostgreSQL");

        public static final IRI DB2 = iri("DB2");

        public static final IRI Informix = iri("Informix");

        public static final IRI Ingres = iri("Ingres");

        public static final IRI Progress = iri("Progress");

        public static final IRI SybaseASE = iri("SybaseASE");

        public static final IRI SybaseSQLAnywhere = iri("SybaseSQLAnywhere");

        public static final IRI Virtuoso = iri("Virtuoso");

        public static final IRI Firebird = iri("Firebird");

        public static final IRI RelativePathSource = iri("RelativePathSource");

        public static final IRI root = iri("root");

        public static final IRI path = iri("path");

        public static final IRI CurrentWorkingDirectory = iri("CurrentWorkingDirectory");

        public static final IRI MappingDirectory = iri("MappingDirectory");

        public static final IRI LogicalTarget = iri("LogicalTarget");

        public static final IRI target = iri("target");

        public static final IRI Target = iri("Target");

        public static final IRI serialization = iri("serialization");

        public static final IRI encoding = iri("encoding");

        public static final IRI compression = iri("compression");

        public static final IRI none = iri("none");

        public static final IRI gzip = iri("gzip");

        public static final IRI zip = iri("zip");

        public static final IRI tarxz = iri("tarxz");

        public static final IRI targz = iri("targz");

        public static final IRI NULL = iri("null");

        public static final IRI strategy = iri("strategy");

        public static final IRI gatherAs = iri("gatherAs");

        public static final IRI gather = iri("gather");

        public static final IRI allowEmptyListAndContainer = iri("allowEmptyListAndContainer");

        public static final IRI Strategy = iri("Strategy");

        public static final IRI append = iri("append");

        public static final IRI cartesianProduct = iri("cartesianProduct");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class OldRml {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.OldRml.NAMESPACE + suffix);
        }

        public static final IRI logicalSource = iri("logicalSource");

        public static final IRI datatypeMap = iri("datatypeMap");

        public static final IRI languageMap = iri("languageMap");

        public static final IRI LogicalSource = iri("LogicalSource");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Ql {

        private static final String NAMESPACE = "http://semweb.mmlab.be/ns/ql#";

        private static IRI iri(String suffix) {
            return f.createIRI(NAMESPACE + suffix);
        }

        public static final IRI JsonPath = iri("JSONPath");

        public static final IRI XPath = iri("XPath");

        public static final IRI Csv = iri("CSV");

        public static final IRI Rdb = iri("Rdb");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Rr {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Rr.NAMESPACE + suffix);
        }

        public static final IRI subject = iri("subject");

        public static final IRI subjectMap = iri("subjectMap");

        public static final IRI predicate = iri("predicate");

        public static final IRI predicateMap = iri("predicateMap");

        public static final IRI object = iri("object");

        public static final IRI objectMap = iri("objectMap");

        public static final IRI graph = iri("graph");

        public static final IRI graphMap = iri("graphMap");

        public static final IRI constant = iri("constant");

        public static final IRI defaultGraph = iri("defaultGraph");

        public static final IRI parentTriplesMap = iri("parentTriplesMap");

        public static final IRI joinCondition = iri("joinCondition");

        public static final IRI datatype = iri("datatype");

        public static final IRI language = iri("language");

        public static final IRI child = iri("child");

        public static final IRI parent = iri("parent");

        public static final IRI TriplesMap = iri("TriplesMap");

        public static final IRI SubjectMap = iri("SubjectMap");

        public static final IRI PredicateMap = iri("PredicateMap");

        public static final IRI ObjectMap = iri("ObjectMap");

        public static final IRI PredicateObjectMap = iri("PredicateObjectMap");

        public static final IRI RefObjectMap = iri("RefObjectMap");

        public static final IRI GraphMap = iri("GraphMap");

        public static final IRI Join = iri("Join");

        public static final IRI Literal = iri("Literal");

        public static final IRI BlankNode = iri("BlankNode");

        public static final IRI IRI = iri("IRI");

        public static final IRI logicalTable = iri("logicalTable");

        public static final IRI LogicalTable = iri("LogicalTable");

        public static final IRI SQL2008 = iri("SQL2008");

        public static final IRI Oracle = iri("Oracle");

        public static final IRI MySQL = iri("MySQL");

        public static final IRI MSSQLServer = iri("MSSQLServer");

        public static final IRI HSQLDB = iri("HSQLDB");

        public static final IRI PostgreSQL = iri("PostgreSQL");

        public static final IRI DB2 = iri("DB2");

        public static final IRI Informix = iri("Informix");

        public static final IRI Ingres = iri("Ingres");

        public static final IRI Progress = iri("Progress");

        public static final IRI SybaseASE = iri("SybaseASE");

        public static final IRI SybaseSQLAnywhere = iri("SybaseSQLAnywhere");

        public static final IRI Virtuoso = iri("Virtuoso");

        public static final IRI Firebird = iri("Firebird");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Fnml {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Fnml.NAMESPACE + suffix);
        }

        public static final IRI functionValue = iri("functionValue");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Fno {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Fno.NAMESPACE + suffix);
        }

        public static final IRI Execution = iri("Execution");

        public static final IRI executes = iri("executes");

        public static final IRI old_executes = f.createIRI(io.carml.vocab.Fno.OLD_executes);
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class D2rq {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.D2rq.NAMESPACE + suffix);
        }

        public static final IRI Database = iri("Database");

        public static final IRI jdbcDriver = iri("jdbcDriver");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Csvw {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Csvw.NAMESPACE + suffix);
        }

        public static final IRI Table = iri("Table");

        public static final IRI Dialect = iri("Dialect");

        public static final IRI rtl = iri("rtl");

        public static final IRI ltr = iri("ltr");

        public static final IRI auto = iri("auto");

        public static final IRI url = iri("url");

        public static final IRI dialect = iri("dialect");
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Carml {

        private static IRI iri(String suffix) {
            return f.createIRI(io.carml.vocab.Carml.NAMESPACE + suffix);
        }

        public static final IRI Stream = iri("Stream");

        public static final IRI streamName = iri("streamName");

        public static final IRI FileSource = iri("FileSource");

        public static final IRI url = iri("url");

        public static final IRI XmlDocument = iri("XmlDocument");

        public static final IRI declaresNamespace = iri("declaresNamespace");

        public static final IRI Namespace = iri("Namespace");

        public static final IRI namespacePrefix = iri("namespacePrefix");

        public static final IRI namespaceName = iri("namespaceName");

        public static final IRI MultiObjectMap = iri("MultiObjectMap");

        public static final IRI multiReference = iri("multiReference");

        public static final IRI multiTemplate = iri("multiTemplate");

        public static final IRI multiFunctionValue = iri("multiFunctionValue");

        public static final IRI multiJoinCondition = iri("multiJoinCondition");
    }
}
