## RMLLVTC0004b

**Title**: Natural Datatype: Index Expression Field

**Description**: Test the natural datatype mapping for the index of an expression field

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
    rml:template "http://example.org/person/{name}/item/{item.type}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasSequenceNumber ;
    rml:objectMap [
      rml:reference "item.#" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice/item/sword> <http://example.org/hasSequenceNumber> "0"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/alice/item/shield> <http://example.org/hasSequenceNumber> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/bob/item/flower> <http://example.org/hasSequenceNumber> "0"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

