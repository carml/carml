## RMLLVTC0002b

**Title**: Iterable Field with Multiple Children

**Description**: Test a nested field construction: iterable field with mulitple expression fields as children

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
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "weight" ;
      rml:reference "$.weight" ;
    ] ;
  ] .

:triplesMapItem a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}/item/{item.type}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasName ;
    rml:objectMap [
      rml:reference "item.type" ;
    ] ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasWeight ;
    rml:objectMap [
      rml:reference "item.weight" ;
      rml:datatype xsd:integer ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice/item/sword> <http://example.org/hasName> "sword" .
<http://example.org/person/alice/item/sword> <http://example.org/hasWeight> "1500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/alice/item/shield> <http://example.org/hasName> "shield" .
<http://example.org/person/alice/item/shield> <http://example.org/hasWeight> "2500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/bob/item/flower> <http://example.org/hasName> "flower" .
<http://example.org/person/bob/item/flower> <http://example.org/hasWeight> "15"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

