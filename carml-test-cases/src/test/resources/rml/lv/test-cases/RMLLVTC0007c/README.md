## RMLLVTC0007c

**Title**: Change Reference Formulations: JSON including CSV

**Description**: Test a change of reference formulations: json source including csv data

**Error expected?** No

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "items": "type,weight\nsword,1500\nshield,2500"
    },
    {
      "name": "bob",
      "items": "type,weight\nflower,15"
    }
  ]
}

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://example.org/> .

:mixedJSONSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.json" ;
  ] ;
  rml:referenceFormulation rml:JSONPath ;
  rml:iterator "$.people[*]" .

:mixedJSONView a rml:LogicalView ;
  rml:viewOn :mixedJSONSource ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "name" ;
    rml:reference "$.name" ;
    ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "items" ;
    rml:reference "$.items" ;
    rml:field [
      a rml:IterableField ;
      rml:referenceFormulation rml:CSV ;
      rml:fieldName "item";
      rml:field [
        a rml:ExpressionField ;
        rml:fieldName "type" ;
        rml:reference "type" ;
      ] ;
      rml:field [
        a rml:expressionField ;
        rml:fieldName "weight" ;
        rml:reference "weight" ;
      ] ;
    ] ;
  ].

:triplesMapItem a rml:TriplesMap ;
  rml:logicalSource :mixedJSONView ;
  rml:subjectMap [
    rml:template "http://example.org/item_{#}_{items.item.#}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasType ;
    rml:objectMap [
      rml:reference "items.item.type" ;
    ] ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasWeight ;
    rml:objectMap [
      rml:reference "items.item.weight" ;
      rml:datatype xsd:integer ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/item_0_0> <http://example.org/hasType> "sword" .
<http://example.org/item_0_0> <http://example.org/hasWeight> "1500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/item_0_1> <http://example.org/hasType> "shield" .
<http://example.org/item_0_1> <http://example.org/hasWeight> "2500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/item_1_0> <http://example.org/hasType> "flower" .
<http://example.org/item_1_0> <http://example.org/hasWeight> "15"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

