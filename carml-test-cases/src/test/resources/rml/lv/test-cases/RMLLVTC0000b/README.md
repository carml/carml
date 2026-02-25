## RMLLVTC0000b

**Title**: Logical view on CSV source

**Description**: Test a view on a tabular source

**Error expected?** No

**Input**
```
name,birthyear
alice,1995
bob,1999
tobias,2005

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix : <http://example.org/> .

:csvSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.csv" ;
  ] ;
  rml:referenceFormulation rml:CSV .

:csvView a rml:LogicalView ;
  rml:viewOn :csvSource ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "name" ;
    rml:reference "name" ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :csvView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasName ;
    rml:objectMap [
      rml:reference "name" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasName> "alice" .
<http://example.org/person/bob> <http://example.org/hasName> "bob" .
<http://example.org/person/tobias> <http://example.org/hasName> "tobias" .
```

