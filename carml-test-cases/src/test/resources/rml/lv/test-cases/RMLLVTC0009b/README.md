## RMLLVTC0009b

**Title**: Name collision: caused by Field from a Join

**Description**: Test a name collision caused by a field from a join

**Error expected?** Yes

**Input**
```
{
  "people": [
    {
      "givenName": "alice",
      "familyName": "smith",
      "items": [
        "sword",
        "shield"
      ]
    },
    {
      "givenName": "bob",
      "familyName": "johnson",
      "items": [
        "flower"
      ]
    }
  ]
}

```

**Input 1**
```
name,birthyear
alice,1995
bob,1999
tobias,2005

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
    rml:fieldName "givenName" ;
    rml:reference "$.givenName" ;
  ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "familyName" ;
    rml:reference "$.familyName" ;
  ] .

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
  ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "birthyear" ;
    rml:reference "birthyear" ;
  ] ;
  rml:leftJoin [
    rml:parentLogicalView :jsonView ;
    rml:joinCondition [
      rml:parent "givenName" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "name" ;
      rml:reference "familyName" ;
    ] ; 
  ] .


:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :csvView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasBirthYear ;
    rml:objectMap [
      rml:reference "birthyear" ;
      rml:datatype xsd:gYear ;
    ] ;
  ] .

```

