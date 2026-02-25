## RMLLVTC0006f

**Title**: Index Key of Field in Join

**Description**: Test references to indexes of fields from a join

**Error expected?** No

**Input**
```
{
  "people": [
    {
      "name": "alice",
      "items": [
        "sword",
        "shield"
      ]
    },
    {
      "name": "bob",
      "items": [
        "flower"
      ]
    }
  ]
}

```

**Input 1**
```
name,birthyear
alice,1995
tobias,2005
bob,1999

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
    a rml:ExpressionField ;
    rml:fieldName "item" ;
    rml:reference "$.items[*]" ;
  ] .

:csvSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.csv" ;
  ] ;
  rml:referenceFormulation rml:CSV .

:csvView a rml:LogicalView ;
  rml:viewOn :csvSource ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "name" ;
    rml:reference "name" ;
  ] ;
  rml:field [
    a rml:ExpressionField ;
    rml:fieldName "birthyear" ;
    rml:reference "birthyear" ;
  ] ;
  rml:innerJoin [
    rml:parentLogicalView :jsonView ;
    rml:joinCondition [
      rml:parent "name" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "json_item" ;
      rml:reference "item" ;
    ] ;
  ] .


:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :csvView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{#}" ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasBirthYear ;
    rml:objectMap [
      rml:reference "birthyear" ;
      rml:datatype xsd:gYear ;
    ] ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :hasItem ;
    rml:objectMap [
      rml:template "http://example.org/person/{#}/item/{json_item.#}/{json_item}" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/0> <http://example.org/hasBirthYear> "1995"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/0> <http://example.org/hasItem> <http://example.org/person/0/item/0/sword> .
<http://example.org/person/0> <http://example.org/hasItem> <http://example.org/person/0/item/1/shield> .
<http://example.org/person/2> <http://example.org/hasBirthYear> "1999"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/2> <http://example.org/hasItem> <http://example.org/person/2/item/0/flower> .


```

