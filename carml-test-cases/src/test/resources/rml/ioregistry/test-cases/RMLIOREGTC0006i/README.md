## RMLIOREGTC0006i

**Title**: SQLServer: Two columns mapping, invalid SQL query

**Description**: Tests the presence of an invalid SQL query

**Error expected?** Yes

**Input**
```
USE TestDB;
EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'
EXEC sp_msforeachtable 'DROP TABLE ?'

CREATE TABLE student (
  "ID" INTEGER,
  "Name" VARCHAR(50)
);
INSERT INTO student values ('10', 'Venus');

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator "SELECT kjnqsdjfbqsdjfmsdnfm FROM student";
      rml:referenceFormulation rml:SQL2008Query;
      rml:source <http://example.com/base/#DB_source>
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
  d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  d2rq:password "YourSTRONG!Passw0rd;";
  d2rq:username "sa" .

```

