## RMLLVTC0008c

**Title**: Cycle: Self-Join

**Description**: Test a cycle in a join of a logical view: self-join

**Error expected?** Yes

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "items": [
        {
          "type": "sword",
          "weight": 1500
        },
        {
          "type": "shield",
          "weight": 2500
        }
      ]
    },
    {
      "name": "bob",
      "items": [
        {
          "type": "flower",
          "weight": 15
        }
      ]
    }
  ]
}

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
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
  rml:leftJoin [
    rml:parentLogicalView :jsonView ;
    rml:joinCondition [
      rml:parent "name" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "newName" ;
      rml:reference "name" ;
    ] ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{newName}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasName ;
    rml:objectMap [
      rml:reference "newName" ;
    ] ;
  ] .

```

