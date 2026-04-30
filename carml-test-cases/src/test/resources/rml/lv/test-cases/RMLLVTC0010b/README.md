## RMLLVTC0010b

**Title**: Expression Map in Join Condition: Constant

**Description**: Test a constant-valued expression map in join condition 

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
      ],
      "race": "human"
    },
    {
      "name": "bob",
      "items": [
        "flower"
      ], 
      "race": "alien"
    }, 
    {
      "name": "tobias",
      "race": "human"
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

**Mapping**
```
@prefix rml: <http://w3id.org/rml/>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix : <http://example.org/>.

:jsonSource a rml:LogicalSource;
  rml:source [
    a rml:RelativePathSource, rml:Source;
    rml:root rml:MappingDirectory;
    rml:path "people.json";
  ];
  rml:referenceFormulation rml:JSONPath;
  rml:iterator "$.people[*]".

:jsonView a rml:LogicalView;
  rml:viewOn :jsonSource;
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "json_name";
    rml:reference "$.name";
  ];
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "json_race";
    rml:reference "$.race";
  ];
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "json_item";
    rml:reference "$.items[*]";
  ].

:csvSource a rml:LogicalSource;
  rml:source [
    a rml:RelativePathSource, rml:Source;
    rml:root rml:MappingDirectory;
    rml:path "people.csv";
  ];
  rml:referenceFormulation rml:CSV.

:csvView a rml:LogicalView;
  rml:viewOn :csvSource;
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "csv_name";
    rml:reference "name";
  ];
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "csv_birthyear";
    rml:reference "birthyear";
  ];
  rml:innerJoin [
    rml:parentLogicalView :jsonView;
    rml:joinCondition [
      rml:parentMap [ rml:reference "json_race" ];
      rml:childMap [ rml:constant "human" ];
    ];
    rml:joinCondition [
      rml:parentMap [ rml:reference "json_name" ];
      rml:childMap [ rml:reference "csv_name" ];
    ];
    rml:field [
      a rml:ExpressionField;
      rml:fieldName "json_item";
      rml:reference "json_item";
    ];
  ].

:triplesMapPerson a rml:TriplesMap;
  rml:logicalSource :csvView;
  rml:subjectMap [ rml:template "http://example.org/person/{csv_name}" ];
  rml:predicateObjectMap [
    rml:predicate :hasBirthYear;
    rml:objectMap [
      rml:reference "csv_birthyear";
      rml:datatype xsd:gYear;
    ];
  ];
  rml:predicateObjectMap [
    rml:predicate :hasItem;
    rml:objectMap [ rml:template "http://example.org/person/{csv_name}/item/{json_item}" ];
  ].


```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasBirthYear> "1995"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/alice> <http://example.org/hasItem> <http://example.org/person/alice/item/shield> .
<http://example.org/person/alice> <http://example.org/hasItem> <http://example.org/person/alice/item/sword> .
<http://example.org/person/tobias> <http://example.org/hasBirthYear> "2005"^^<http://www.w3.org/2001/XMLSchema#gYear> .

```

