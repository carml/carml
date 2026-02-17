## RMLIOREGTC0012i

**Title**: CSVW Quote Character

**Description**: Define quote character for columns

**Error expected?** No

**Input**
```
'id','name','age'
0,Monica Geller,33
1,Rachel Green,34
2,Joey Tribbiani,35
3,Chandler Bing,36
4,Ross Geller,37

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
   csvw:url "Friends.csv" ;
   csvw:dialect [ a csvw:Dialect;
       csvw:quoteChar "'";
   ] .

```

**Output**
```
<http://example.org/0> <http://xmlns.com/foaf/0.1/age> "33" .
<http://example.org/0> <http://xmlns.com/foaf/0.1/name> "Monica Geller" .
<http://example.org/1> <http://xmlns.com/foaf/0.1/age> "34" .
<http://example.org/1> <http://xmlns.com/foaf/0.1/name> "Rachel Green" .
<http://example.org/2> <http://xmlns.com/foaf/0.1/age> "35" .
<http://example.org/2> <http://xmlns.com/foaf/0.1/name> "Joey Tribbiani" .
<http://example.org/3> <http://xmlns.com/foaf/0.1/age> "36" .
<http://example.org/3> <http://xmlns.com/foaf/0.1/name> "Chandler Bing" .
<http://example.org/4> <http://xmlns.com/foaf/0.1/age> "37" .
<http://example.org/4> <http://xmlns.com/foaf/0.1/name> "Ross Geller" .

```

