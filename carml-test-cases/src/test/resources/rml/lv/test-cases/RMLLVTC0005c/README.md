## RMLLVTC0005c

**Title**: Referencing the Record Key of an Iterable Field

**Description**: Test a reference to the record key of an iterable field

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
    a rml:IterableField ;
    rml:fieldName "item" ;
    rml:iterator "$.items[*]" ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "type" ;
      rml:reference "$.type" ;
    ] ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasItem ;
    rml:objectMap [
      rml:template "http://example.org/person/{name}/{item}" ;
    ] ;
  ] .

```

