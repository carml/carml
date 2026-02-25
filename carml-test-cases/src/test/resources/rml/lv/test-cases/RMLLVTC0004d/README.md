## RMLLVTC0004d

**Title**: Natural Datatype: Record Expression Field

**Description**: Test the natural datatype mapping  for a record of an expression field

**Error expected?** No

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "birth_year": 1995
    },
    {
      "name": "bob",
      "birth_year": 1999
    }
  ]
}

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://example.org/> .

:jsonSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.json" ;
  ] ;
  rml:referenceFormulation rml:JSONPath ;
  rml:iterator "$.people[*]" .

:jsonView a rml:LogicalView ;
  rml:viewOn :jsonSource ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "name" ;
    rml:reference "$.name" ;
  ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "birthyear" ;
    rml:reference "$.birth_year" ;
  ] .

:triplesMapItem a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasBirthYear ;
    rml:objectMap [
      rml:reference "birthyear" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasBirthYear> "1995"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/bob> <http://example.org/hasBirthYear> "1999"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

