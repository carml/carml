## RMLIOREGTC0006q

**Title**: SQLServer: Named column in logical table

**Description**: Test a logical table named column.

**Error expected?** No

**Input**
```
USE TestDB;
DROP TABLE IF EXISTS Student;
DROP TABLE IF EXISTS Sport;

CREATE TABLE Sport (
ID integer,
Name varchar (50),
PRIMARY KEY (ID)
);

CREATE TABLE Student (
ID integer,
Name varchar(50),
Sport integer,
PRIMARY KEY (ID),
FOREIGN KEY(Sport) REFERENCES Sport(ID)
);

INSERT INTO Sport (ID, Name) VALUES (100,'Tennis');
INSERT INTO Student (ID, Name, Sport) VALUES (10,'Venus Williams', 100);
INSERT INTO Student (ID, Name, Sport) VALUES (20,'Demi Moore', NULL);

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator """
        SELECT Name, COUNT(Sport) as SPORTCOUNT
        FROM Student
        GROUP BY Name
        """;
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Query
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "Name"
        ];
      rml:predicate foaf:name
    ], [
      rml:objectMap [
          rml:reference "SPORTCOUNT"
        ];
      rml:predicate ex:numSport
    ];
  rml:subjectMap [
      rml:template "http://example.com/resource/student_{Name}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  d2rq:password "YourSTRONG!Passw0rd;";
  d2rq:username "sa" .

```

**Output**
```
<http://example.com/resource/student_Venus%20Williams> <http://xmlns.com/foaf/0.1/name> "Venus Williams" . 
<http://example.com/resource/student_Venus%20Williams> <http://example.com/numSport> "1"^^<http://www.w3.org/2001/XMLSchema#integer> . 
<http://example.com/resource/student_Demi%20Moore> <http://xmlns.com/foaf/0.1/name> "Demi Moore" . 
<http://example.com/resource/student_Demi%20Moore> <http://example.com/numSport> "0"^^<http://www.w3.org/2001/XMLSchema#integer> . 




```

