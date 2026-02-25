## RMLLVTC0001d

**Title**: Expression Field Siblings

**Description**: Test multiple expression fields with the same parent

**Error expected?** No

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "lastName": "smith",
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
      "lastName": "jones",
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
    rml:fieldName "lastName" ;
    rml:reference "$.lastName" ;
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasFullName ;
    rml:objectMap [
      rml:template "{name} {lastName}" ;
      rml:termType rml:Literal ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasFullName> "alice smith" .
<http://example.org/person/bob> <http://example.org/hasFullName> "bob jones" .

```

