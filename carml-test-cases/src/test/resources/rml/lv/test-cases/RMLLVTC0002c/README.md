## RMLLVTC0002c

**Title**: Nested Iterable Fields

**Description**: Test a nested field construction: iterable field with an iterable field as child

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
          "measures": {
            "weight": 1500,
            "length": 30
          }
        },
        {
          "type": "shield",
          "measures": {
            "weight": 2500
          }
        }
      ]
    },
    {
      "name": "bob",
      "items": [
        {
          "type": "flower",
          "measures" : {
            "weight": 15,
            "length" : 15
          }
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
      rml:fieldName "type" ;
      rml:reference "$.type" ;
    ] ;
    rml:field [
      a rml:IterableField ;
      rml:fieldName "measures";
      rml:iterator "$.measures";
      rml:field [
        a rml:ExpressionField ;
        rml:fieldName "weight" ;
        rml:reference "$.weight" ;
        ];
    ] ;
  ] .

:triplesMapItem a rml:TriplesMap ;
  rml:logicalSource :jsonView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{name}/item/{item.type}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasWeight ;
    rml:objectMap [
      rml:reference "item.measures.weight" ;
      rml:datatype xsd:integer ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/alice/item/sword> <http://example.org/hasWeight> "1500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/alice/item/shield> <http://example.org/hasWeight> "2500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/person/bob/item/flower> <http://example.org/hasWeight> "15"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

