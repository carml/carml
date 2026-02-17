## RMLIOREGTC0004r

**Title**: MySQL: M to M relation, by using a SQL query

**Description**: Tests, M to M relations, by using a SQL query

**Error expected?** No

**Input**
```
USE test;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS test.Student_Sport;
DROP TABLE IF EXISTS test.Sport;
DROP TABLE IF EXISTS test.Student;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE Student (
ID integer PRIMARY KEY,
FirstName varchar(50),
LastName varchar(50)
);
CREATE TABLE Sport (
ID integer PRIMARY KEY,
Description varchar(50)
);
CREATE TABLE Student_Sport (
ID_Student integer,
ID_Sport integer,
PRIMARY KEY (ID_Student,ID_Sport),
FOREIGN KEY (ID_Student) REFERENCES Student(ID),
FOREIGN KEY (ID_Sport) REFERENCES Sport(ID)
);

INSERT INTO Student (ID,FirstName,LastName) VALUES (10,'Venus', 'Williams');
INSERT INTO Student (ID,FirstName,LastName) VALUES (11,'Fernando', 'Alonso');
INSERT INTO Student (ID,FirstName,LastName) VALUES (12,'David', 'Villa');

INSERT INTO Sport (ID, Description) VALUES (110,'Tennis');
INSERT INTO Sport (ID, Description) VALUES (111,'Football');
INSERT INTO Sport (ID, Description) VALUES (112,'Formula1');

INSERT INTO Student_Sport (ID_Student, ID_Sport) VALUES (10,110);
INSERT INTO Student_Sport (ID_Student, ID_Sport) VALUES (11,111);
INSERT INTO Student_Sport (ID_Student, ID_Sport) VALUES (11,112);
INSERT INTO Student_Sport (ID_Student, ID_Sport) VALUES (12,111);

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator """
       SELECT Student.ID as ID,
              Student.FirstName as FirstName,
              Student.LastName as LastName,
              Sport.Description as Description,
              Sport.ID as Sport_ID
       FROM Student,Sport,Student_Sport
       WHERE Student.ID = Student_Sport.ID_Student
       AND Sport.ID = Student_Sport.ID_Sport;
       """;
      rml:referenceFormulation rml:SQL2008Query;
      rml:source <http://example.com/base/#DB_source>
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "ID"
        ];
      rml:predicate ex:id
    ], [
      rml:objectMap [
          rml:reference "FirstName"
        ];
      rml:predicate ex:firstName
    ], [
      rml:objectMap [
          rml:reference "LastName"
        ];
      rml:predicate ex:lastName
    ], [
      rml:objectMap [
          rml:template "http://example.com/{Sport_ID}/{Description}"
        ];
      rml:predicate ex:plays
    ];
  rml:subjectMap [
      rml:template "http://example.com/{ID}/{FirstName};{LastName}"
    ] .

<http://example.com/base/TriplesMap2> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator """
       SELECT * FROM Sport;
       """;
      rml:referenceFormulation rml:SQL2008Query;
      rml:source <http://example.com/base/#DB_source>
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "ID"
        ];
      rml:predicate ex:id
    ], [
      rml:objectMap [
          rml:reference "Description"
        ];
      rml:predicate ex:description
    ];
  rml:subjectMap [
      rml:template "http://example.com/{ID}/{Description}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver";
  d2rq:password "";
  d2rq:username "root" .

```

**Output**
```
<http://example.com/110/Tennis> <http://example.com/description> "Tennis" .
<http://example.com/110/Tennis> <http://example.com/id> "110"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/10/Venus;Williams> <http://example.com/plays> <http://example.com/110/Tennis> .
<http://example.com/10/Venus;Williams> <http://example.com/lastName> "Williams" .
<http://example.com/10/Venus;Williams> <http://example.com/firstName> "Venus" .
<http://example.com/10/Venus;Williams> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/111/Football> <http://example.com/description> "Football" .
<http://example.com/111/Football> <http://example.com/id> "111"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/12/David;Villa> <http://example.com/plays> <http://example.com/111/Football> .
<http://example.com/12/David;Villa> <http://example.com/lastName> "Villa" .
<http://example.com/12/David;Villa> <http://example.com/firstName> "David" .
<http://example.com/12/David;Villa> <http://example.com/id> "12"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/112/Formula1> <http://example.com/description> "Formula1" .
<http://example.com/112/Formula1> <http://example.com/id> "112"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/11/Fernando;Alonso> <http://example.com/lastName> "Alonso" .
<http://example.com/11/Fernando;Alonso> <http://example.com/firstName> "Fernando" .
<http://example.com/11/Fernando;Alonso> <http://example.com/id> "11"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/11/Fernando;Alonso> <http://example.com/plays> <http://example.com/111/Football> .
<http://example.com/11/Fernando;Alonso> <http://example.com/plays> <http://example.com/112/Formula1> .


```

