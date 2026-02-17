## RMLIOREGTC0005u

**Title**: PostgreSQL: three tables, one primary key, one foreign key

**Description**: Test the translation of database type codes to IRIs

**Error expected?** No

**Input**
```
DROP TABLE IF EXISTS LIKES;
DROP TABLE IF EXISTS EMP;
DROP TABLE IF EXISTS DEPT;

CREATE TABLE DEPT (
      deptno INTEGER UNIQUE,
      dname VARCHAR(30),
      loc VARCHAR(100));
INSERT INTO DEPT (deptno, dname, loc) VALUES (10, 'APPSERVER', 'NEW YORK');

CREATE TABLE EMP (
      empno INTEGER PRIMARY KEY,
      ename VARCHAR(100),
      job VARCHAR(30),
	  deptno INTEGER REFERENCES DEPT (deptno),
	  etype VARCHAR(30));
INSERT INTO EMP (empno, ename, job, deptno, etype ) VALUES (7369, 'SMITH', 'CLERK', 10, 'PART_TIME');

CREATE TABLE LIKES (
      id INTEGER,
      likeType VARCHAR(30),
      likedObj VARCHAR(100));
INSERT INTO LIKES (id, likeType, likedObj) VALUES (7369, 'Playing', 'Soccer');
INSERT INTO LIKES (id, likeType, likedObj) VALUES (7369, 'Watching', 'Basketball');

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/ns#> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator """
      SELECT EMP.*, (CASE job
        WHEN 'CLERK' THEN 'general-office'
        WHEN 'NIGHTGUARD' THEN 'security'
        WHEN 'ENGINEER' THEN 'engineering'
      END) AS ROLE FROM EMP
    """;
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Query
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:template "http://data.example.com/roles/{role}"
        ];
      rml:predicate ex:role
    ];
  rml:subjectMap [
      rml:template "http://data.example.com/employee/{empno}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "org.postgresql.Driver";
  d2rq:password "";
  d2rq:username "postgres" .

```

**Output**
```
<http://data.example.com/employee/7369> <http://example.com/ns#role> <http://data.example.com/roles/general-office> .

```

