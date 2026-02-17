## RMLIOREGTC0006t

**Title**: SQLServer: Default mapping

**Description**: Tests the generation of a default mapping for tables without a primary key.

**Error expected?** No

**Input**
```
USE TestDB;
DROP TABLE IF EXISTS IOUs;
DROP TABLE IF EXISTS Lives;

CREATE TABLE IOUs (
      fname VARCHAR(20),
      lname VARCHAR(20),
      amount FLOAT);
INSERT INTO IOUs (fname, lname, amount) VALUES ('Bob', 'Smith', 30);
INSERT INTO IOUs (fname, lname, amount) VALUES ('Sue', 'Jones', 20);
INSERT INTO IOUs (fname, lname, amount) VALUES ('Bob', 'Smith', 30);

CREATE TABLE Lives (
      fname VARCHAR(20),
      lname VARCHAR(20),
      city VARCHAR(20));
INSERT INTO Lives (fname, lname, city) VALUES ('Bob', 'Smith', 'London');
INSERT INTO Lives (fname, lname, city) VALUES ('Sue', 'Jones', 'Madrid');
INSERT INTO Lives (fname, lname, city) VALUES ('Bob', 'Smith', 'London');

```

**Mapping**
```
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Table;
      rml:iterator "IOUs"
    ];
  rml:predicateObjectMap [
      rml:object <http://example.com/base/IOUs>;
      rml:predicate rdf:type
    ], [
      rml:objectMap [
          rml:reference "fname"
        ];
      rml:predicate <http://example.com/base/IOUs#fname>
    ], [
      rml:objectMap [
          rml:reference "lname"
        ];
      rml:predicate <http://example.com/base/IOUs#lname>
    ], [
      rml:objectMap [
          rml:reference "amount"
        ];
      rml:predicate <http://example.com/base/IOUs#amount>
    ];
  rml:subjectMap [
      rml:template "{fname}_{lname}_{amount}";
      rml:termType rml:BlankNode
    ] .

<http://example.com/base/TriplesMap2> a rml:TriplesMap;
  rml:logicalSource [
      rml:source <http://example.com/base/#DB_source>;
      rml:referenceFormulation rml:SQL2008Table;
      rml:iterator "Lives"
    ];
  rml:predicateObjectMap [
      rml:object <http://example.com/base/Lives>;
      rml:predicate rdf:type
    ], [
      rml:objectMap [
          rml:reference "fname"
        ];
      rml:predicate <http://example.com/base/IOUs#fname>
    ], [
      rml:objectMap [
          rml:reference "lname"
        ];
      rml:predicate <http://example.com/base/IOUs#lname>
    ], [
      rml:objectMap [
          rml:reference "city"
        ];
      rml:predicate <http://example.com/base/IOUs#city>
    ];
  rml:subjectMap [
      rml:template "{fname}_{lname}_{city}";
      rml:termType rml:BlankNode
    ] .

<http://example.com/base/#DB_source> a d2rq:Database;
  d2rq:jdbcDSN "CONNECTIONDSN";
  d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  d2rq:password "YourSTRONG!Passw0rd;";
  d2rq:username "sa" .

```

**Output**
```
_:Bob_Smith_30 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/base/IOUs> .
_:Bob_Smith_30 <http://example.com/base/IOUs#fname> "Bob" .
_:Bob_Smith_30 <http://example.com/base/IOUs#lname> "Smith" .
_:Bob_Smith_30 <http://example.com/base/IOUs#amount> "30.0"^^<http://www.w3.org/2001/XMLSchema#double> .
_:Sue_Jones_20 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/base/IOUs> .
_:Sue_Jones_20 <http://example.com/base/IOUs#fname> "Sue" .
_:Sue_Jones_20 <http://example.com/base/IOUs#lname> "Jones" .
_:Sue_Jones_20 <http://example.com/base/IOUs#amount> "20.0"^^<http://www.w3.org/2001/XMLSchema#double> .
_:Bob_Smith_London <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/base/Lives> .
_:Bob_Smith_London <http://example.com/base/IOUs#fname> "Bob" .
_:Bob_Smith_London <http://example.com/base/IOUs#lname> "Smith" .
_:Bob_Smith_London <http://example.com/base/IOUs#city> "London" .
_:Sue_Jones_Madrid <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/base/Lives> .
_:Sue_Jones_Madrid <http://example.com/base/IOUs#fname> "Sue" .
_:Sue_Jones_Madrid <http://example.com/base/IOUs#lname> "Jones" .
_:Sue_Jones_Madrid <http://example.com/base/IOUs#city> "Madrid" .



```

