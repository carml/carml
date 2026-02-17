## RMLIOREGTC0005h

**Title**: PostgreSQL: Two columns mapping, delimited identifiers referenced as regular identifiers

**Description**: Tests the presence of delimited identifiers referenced as regular identifiers. Within rml:template ID is ok, but Name is not

**Error expected?** Yes

**Input**
```
DROP TABLE IF EXISTS Student CASCADE;
CREATE TABLE Student (
ID INTEGER,
Name VARCHAR(15)
);
INSERT INTO Student (ID, Name) VALUES(10,'Venus');

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [
      rml:referenceFormulation rml:SQL2008Table;
      rml:source <http://example.com/base/#DB_source>;
      rml:iterator "\"Student\""
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "\"ID\""
        ];
      rml:predicate ex:id
    ], [
      rml:objectMap [
          rml:reference "\"Name\""
        ];
      rml:predicate foaf:name
    ];
  rml:subjectMap [
      rml:class foaf:Person;
      rml:template "http://example.com/{\"ID\"}/{\"Name\"}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver";
  d2rq:password "";
  d2rq:username "root" .

```

