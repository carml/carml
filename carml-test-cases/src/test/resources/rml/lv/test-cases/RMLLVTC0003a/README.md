## RMLLVTC0003a

**Title**: Index Key: #

**Description**: Test a reference to the index key #

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
  ] .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasSequenceNumber ;
    rml:objectMap [
      rml:reference "#" ;
      rml:datatype xsd:integer ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasSequenceNumber> "0"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/bob> <http://example.org/hasSequenceNumber> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

