## RMLLVTC0009c

**Title**: Name collision: between Fields from different Joins

**Description**: Test a name collision between fields from different joins

**Error expected?** Yes

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "items": [
        "sword",
        "shield"
      ]
    },
    {
      "name": "bob",
      "items": [
        "flower"
      ]
    }
  ]
}

```

**Input 1**
```
name,id
alice,123
bob,456
tobias,789

```

**Input 2**
```
name,item
alice,lantern
bob,leaf
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
    rml:fieldName "jsonitem" ;
    rml:reference "$.items[*]" ;
  ] .

:additionalCsvSource a rml:LogicalSource ;
  rml:source [
   a rml:RelativePathSource , rml:Source ;
   rml:root rml:MappingDirectory ;
   rml:path "people2.csv" ;
  ] ;
  rml:referenceFormulation rml:CSV .

:additionalCsvView a rml:LogicalView ;
  rml:viewOn :additionalCsvSource ;
    rml:field [
     a rml:ExpressionField ;
     rml:fieldName "name" ;
     rml:reference "name" ;
    ] ;
    rml:field [
     a rml:ExpressionField ;
     rml:fieldName "csvitem" ;
     rml:reference "item" ;
    ] ;
.

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
    rml:fieldName "id" ;
    rml:reference "id" ;
  ] ;
  rml:leftJoin [
    rml:parentLogicalView :jsonView ;
    rml:joinCondition [
      rml:parent "name" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "item" ;
      rml:reference "jsonitem" ;
    ] ;
  ] ;
  rml:leftJoin [
    rml:parentLogicalView :additionalCsvView ;
    rml:joinCondition [
      rml:parent "name" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "item" ;
      rml:reference "csvitem" ;
    ] ;
  ] .


:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :csvView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{id}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasItem ;
    rml:objectMap [
      rml:template "http://example.org/person/{id}/item/{item}" ;
    ] ;
  ] .

```

