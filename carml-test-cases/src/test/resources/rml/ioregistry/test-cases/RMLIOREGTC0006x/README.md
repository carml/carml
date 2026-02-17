## RMLIOREGTC0006x

**Title**: SQLServer: Table with datatypes: date and timestamp

**Description**: Tests the rml:termType and datatype conversions: date and timestamp

**Error expected?** No

**Input**
```
USE TestDB;
DROP TABLE IF EXISTS Patient;

CREATE TABLE Patient (
ID INTEGER,
FirstName VARCHAR(50),
LastName VARCHAR(50),
Sex VARCHAR(6),
Weight REAL,
Height FLOAT,
BirthDate DATE,
EntranceDate DATETIME,
PaidInAdvance BIT,
Photo VARBINARY(200),
PRIMARY KEY (ID)
);

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (10,'Monica','Geller','female',80.25,1.65,'1981-10-10','2009-10-10 12:12:22',0,
CAST('89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFEBFC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082' as VARBINARY));

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (11,'Rachel','Green','female',70.22,1.70,'1982-11-12','2008-11-12 09:45:44',1,
CAST('89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFF3FC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082' as VARBINARY));

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (12,'Chandler','Bing','male',90.31,1.76,'1978-04-06','2007-03-12 02:13:14',1,
CAST('89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFEBFC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082' as VARBINARY));

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Table;
      rml:iterator "Patient"
    ];
  rml:predicateObjectMap [
      rml:object foaf:Person;
      rml:predicate rdf:type
    ], [
      rml:objectMap [
          rml:reference "BirthDate"
        ];
      rml:predicate ex:birthdate
    ], [
      rml:objectMap [
          rml:reference "EntranceDate"
        ];
      rml:predicate ex:entrancedate
    ];
  rml:subjectMap [
      rml:template "http://example.com/Patient{ID}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  d2rq:password "YourSTRONG!Passw0rd;";
  d2rq:username "sa" .

```

**Output**
```
<http://example.com/Patient10> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient10> <http://example.com/birthdate> "1981-10-10"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://example.com/Patient10> <http://example.com/entrancedate> "2009-10-10T12:12:22"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.com/Patient11> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient11> <http://example.com/birthdate> "1982-11-12"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://example.com/Patient11> <http://example.com/entrancedate> "2008-11-12T09:45:44"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.com/Patient12> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient12> <http://example.com/birthdate> "1978-04-06"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://example.com/Patient12> <http://example.com/entrancedate> "2007-03-12T02:13:14"^^<http://www.w3.org/2001/XMLSchema#dateTime> .

```

