## RMLIOREGTC0012d

**Title**: CSVW trim

**Description**: Apply trimming on CSV values

**Error expected?** No

**Input**
```
ID,Name
  10  ,Venus

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix csvw: <http://www.w3.org/ns/csvw#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ex: <http://example.com/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@base <http://example.com/base/> .

<TriplesMap1> a rml:TriplesMap;

  rml:logicalSource [
    rml:source <#CSVW_source>;
    rml:referenceFormulation rml:CSV
  ];

  rml:subjectMap [
    rml:template "http://example.com/{ID}/{Name}";
    rml:class foaf:Person
  ];

  rml:predicateObjectMap [
    rml:predicate ex:id ;
    rml:objectMap [ rml:reference "ID" ]
  ];

  rml:predicateObjectMap [
    rml:predicate foaf:name ;
    rml:objectMap [ rml:reference "Name" ]
  ].

<#CSVW_source> a csvw:Table;
   csvw:url "student.csv" ;
   csvw:dialect [ a csvw:Dialect;
       csvw:trim true;
   ] .

```

**Output**
```
<http://example.com/10/Venus> <http://xmlns.com/foaf/0.1/name> "Venus" .
<http://example.com/10/Venus> <http://example.com/id> "10" .
<http://example.com/10/Venus> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .


```

