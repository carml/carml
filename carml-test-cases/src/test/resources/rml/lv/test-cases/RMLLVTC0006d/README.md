## RMLLVTC0006d

**Title**: Inner Join and Left Join

**Description**: Test an inner join and a left join in one logical view

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
bob,1999
tobias,2005

```

**Input 2**
```
name,id
alice,123
bob,456
tobias,789

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

:additionalCsvSource a rml:LogicalSource ;
  rml:source [
   a rml:RelativePathSource , rml:Source ;
   rml:root rml:MappingDirectory ;
   rml:path "people2.csv" ;
  ] ;
  rml:referenceFormulation rml:CSV .

:additionalCsvView a rml:LogicalView ;
  rml:viewOn :additionalCsvSource ;
    rml:field [
     a rml:ExpressionField ;
     rml:fieldName "name" ;
     rml:reference "name" ;
    ] ;
    rml:field [
     a rml:ExpressionField ;
     rml:fieldName "id" ;
     rml:reference "id" ;
    ] ;
.

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
  ] ;
  rml:leftJoin [
    rml:parentLogicalView :additionalCsvView ;
    rml:joinCondition [
      rml:parent "name" ;
      rml:child "name" ;
    ] ;
    rml:field [
      a rml:ExpressionField ;
      rml:fieldName "csv2_id" ;
      rml:reference "id" ;
    ] ;
  ] .


:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :csvView ;
  rml:subjectMap [
    rml:template "http://example.org/person/{csv2_id}" ;
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
      rml:template "http://example.org/person/{csv2_id}/item/{json_item}" ;
    ] ;
  ] .

```

**Output**
```
<http://example.org/person/123> <http://example.org/hasBirthYear> "1995"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/123> <http://example.org/hasItem> <http://example.org/person/123/item/sword> .
<http://example.org/person/123> <http://example.org/hasItem> <http://example.org/person/123/item/shield> .
<http://example.org/person/456> <http://example.org/hasBirthYear> "1999"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/456> <http://example.org/hasItem> <http://example.org/person/456/item/flower> .

```

