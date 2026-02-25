## RMLLVTC0000c

**Title**: Logical View on Logical View

**Description**: Test a view on a logical view

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
  ] .

:jsonViewOnJsonView a rml:LogicalView ;
  rml:viewOn :jsonView ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "newName" ;
    rml:reference "name" ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonViewOnJsonView ;
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

**Output**
```
<http://example.org/person/alice> <http://example.org/hasName> "alice" .
<http://example.org/person/bob> <http://example.org/hasName> "bob" .

```

