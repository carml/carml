## RMLIOREGTC0006e

**Title**: SQLServer query invalid

**Description**: Handle invalid SQL query for SQLServer

**Error expected?** Yes

**Input**
```
USE TestDB;
DROP TABLE IF EXISTS TestDB.Person;

CREATE TABLE Person (
ID integer,
Name varchar(50),
DateOfBirth varchar(50),
PRIMARY KEY (ID)
);
INSERT INTO Person (ID, Name, DateOfBirth) VALUES (1,'Alice', 'September, 2021');
INSERT INTO Person (ID, Name, DateOfBirth) VALUES (2,'Bob', 'September, 2010');


```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Query;
      rml:iterator "SELECT * FROMasflijafo Person;"
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "DateOfBirth"
        ];
      rml:predicate ex:BirthDay
    ];
  rml:subjectMap [
      rml:template "http://example.com/Person/{ID}/{Name}/{DateOfBirth}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  d2rq:password "YourSTRONG!Passw0rd;";
  d2rq:username "sa" .

```

