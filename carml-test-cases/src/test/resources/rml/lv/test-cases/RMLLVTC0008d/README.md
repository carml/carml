## RMLLVTC0008d

**Title**: Cycle: Fields

**Description**: Test a cycle in nested fields

**Error expected?** Yes

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
  rml:field :field1 .

:field1 a rml:IterableField ;
  rml:fieldName "item" ;
  rml:referenceFormulation rml:JSONPath ;
  rml:iterator "$.items[*]" ;
  rml:field :field2.

:field2 a rml:ExpressionField ;
  rml:fieldName "type" ;
  rml:reference "$.type" ;
  rml:field :field1.

```

