## RMLIOREGTC0004c

**Title**: MySQL NULL

**Description**: Handle MySQL NULL value

**Error expected?** No

**Input**
```
USE test;
DROP TABLE IF EXISTS test.Person;

CREATE TABLE Person (
ID integer,
Name varchar(50),
DateOfBirth varchar(50),
PRIMARY KEY (ID)
);
INSERT INTO Person (ID, Name, DateOfBirth) VALUES (1,'Alice', NULL);
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
      rml:referenceFormulation rml:SQL2008Table;
      rml:iterator "Person"
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
  d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver";
  d2rq:password "";
  d2rq:username "root" .

```

**Output**
```
<http://example.com/Person/2/Bob/September%2C%202010> <http://example.com/BirthDay> "September, 2010" .



```

