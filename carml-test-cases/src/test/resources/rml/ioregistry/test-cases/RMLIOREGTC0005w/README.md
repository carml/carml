## RMLIOREGTC0005w

**Title**: PostgreSQL: Table with datatypes: real and float

**Description**: Tests the rml:termType and datatype conversions: real and float

**Error expected?** No

**Input**
```
DROP TABLE IF EXISTS Patient;

CREATE TABLE Patient (
ID INTEGER,
FirstName VARCHAR(50),
LastName VARCHAR(50),
Sex VARCHAR(6),
Weight REAL,
Height FLOAT,
BirthDate DATE,
EntranceDate TIMESTAMP,
PaidInAdvance BOOLEAN,
Photo BYTEA,
PRIMARY KEY (ID)
);

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (10,'Monica','Geller','female',80.25,1.65,'1981-10-10','2009-10-10 12:12:22',FALSE,
'\\x89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFEBFC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082');

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (11,'Rachel','Green','female',70.22,1.70,'1982-11-12','2008-11-12 09:45:44',TRUE,
'\\x89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFF3FC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082');

INSERT INTO Patient (ID, FirstName,LastName,Sex,Weight,Height,BirthDate,EntranceDate,PaidInAdvance,Photo)
VALUES (12,'Chandler','Bing','male',90.31,1.76,'1978-04-06','2007-03-12 02:13:14',TRUE,
'\\x89504E470D0A1A0A0000000D49484452000000050000000508060000008D6F26E50000001C4944415408D763F9FFFEBFC37F062005C3201284D031F18258CD04000EF535CBD18E0E1F0000000049454E44AE426082');

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
          rml:reference "weight"
        ];
      rml:predicate ex:weight
    ], [
      rml:objectMap [
          rml:reference "height"
        ];
      rml:predicate ex:height
    ];
  rml:subjectMap [
      rml:template "http://example.com/Patient{id}"
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "org.postgresql.Driver";
  d2rq:password "";
  d2rq:username "postgres" .

```

**Output**
```
<http://example.com/Patient10> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient10> <http://example.com/weight> "80.25"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.com/Patient10> <http://example.com/height> "1.65"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.com/Patient11> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient11> <http://example.com/weight> "70.22"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.com/Patient11> <http://example.com/height> "1.7"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.com/Patient12> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Patient12> <http://example.com/weight> "90.31"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.com/Patient12> <http://example.com/height> "1.76"^^<http://www.w3.org/2001/XMLSchema#double> .

```

