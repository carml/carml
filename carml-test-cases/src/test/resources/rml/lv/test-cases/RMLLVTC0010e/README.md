## RMLLVTC0010e

**Title**: Expression Map in Joined Field: Constant

**Description**: Test a constant-valued expression map in joined field 

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
.

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
      rml:parentMap [ rml:reference "json_name" ];
      rml:childMap [ rml:reference "csv_name" ];
    ];
    rml:field [
      a rml:ExpressionField;
      rml:fieldName "status";
      rml:constant "student";
    ];
  ].

:triplesMapPerson a rml:TriplesMap;
  rml:logicalSource :csvView;
  rml:subjectMap [ rml:template "http://example.org/person/{csv_name}" ];
  rml:predicateObjectMap [
    rml:predicate :hasStatus;
    rml:objectMap [ rml:reference "status" ];
  ].


```

**Output**
```
<http://example.org/person/alice> <http://example.org/hasStatus> "student" .
<http://example.org/person/bob> <http://example.org/hasStatus> "student" .

```

