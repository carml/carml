## RMLLVTC0001b

**Title**: Expression Field: Template

**Description**: Test a template in an expression field

**Error expected?** No

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
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "race" ;
    rml:constant "human" ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasRace ;
    rml:objectMap [
      rml:reference "race" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasRace> "human" .
<http://example.org/person/bob> <http://example.org/hasRace> "human" .

```

