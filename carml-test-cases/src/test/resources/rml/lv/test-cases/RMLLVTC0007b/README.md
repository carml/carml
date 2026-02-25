## RMLLVTC0007b

**Title**: Change Reference Formulations: CSV including JSON object

**Description**: Test a change of reference formulations: csv source including json object

**Error expected?** No

**Input**
```
name,item
alice,"{""type"":""sword"",""weight"":1500}"
alice,"{""type"":""shield"",""weight"":2500}"
bob,"{""type"":""flower"",""weight"":15}"

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://example.org/> .

:mixedCSVSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.csv" ;
  ] ;
  rml:referenceFormulation rml:CSV .

:mixedCSVView a rml:LogicalView ;
  rml:viewOn :mixedCSVSource ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "name" ;
    rml:reference "name" ;
  ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "item" ;
    rml:reference "item" ;
    rml:field [
      a rml:IterableField ;
      rml:referenceFormulation rml:JSONPath ;
      rml:iterator "$" ;
      rml:fieldName "itemJson" ;
      rml:field [
        a rml:ExpressionField;
        rml:fieldName "type" ;
        rml:reference "$.type" ; ] ;
      rml:field [
        a rml:ExpressionField;
        rml:fieldName "weight" ;
        rml:reference "$.weight" ;
      ] ;
    ] ;
  ] .

:triplesMapItem a rml:TriplesMap ;
  rml:logicalSource :mixedCSVView ;
  rml:subjectMap [
    rml:template "http://example.org/item_{#}_{item.#}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasType ;
    rml:objectMap [
      rml:reference "item.itemJson.type" ;
    ] ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasWeight ;
    rml:objectMap [
      rml:reference "item.itemJson.weight" ;
      rml:datatype xsd:integer ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/item_0_0> <http://example.org/hasType> "sword" .
<http://example.org/item_0_0> <http://example.org/hasWeight> "1500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/item_1_0> <http://example.org/hasType> "shield" .
<http://example.org/item_1_0> <http://example.org/hasWeight> "2500"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/item_2_0> <http://example.org/hasType> "flower" .
<http://example.org/item_2_0> <http://example.org/hasWeight> "15"^^<http://www.w3.org/2001/XMLSchema#integer> .

```

