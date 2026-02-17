## RMLIOREGTC0004g

**Title**: MySQL: Two columns mapping, an undefined rml:path

**Description**: Tests the presence of an undefined rml:path

**Error expected?** Yes

**Input**
```
USE test;
DROP TABLE IF EXISTS test.student;
CREATE TABLE student (
  ID INTEGER,
  Name VARCHAR(50)
);
INSERT INTO student values ('10', 'Venus');

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [
      rml:referenceFormulation rml:SQL2008Table;
      rml:source <http://example.com/base/#DB_source>;
      rml:iterator "\"Students\""
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "IDs"
        ];
      rml:predicate ex:id
    ];
  rml:subjectMap [
      rml:template "http://example.com/{ID}/{Name}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver";
  d2rq:password "";
  d2rq:username "root" .

```

