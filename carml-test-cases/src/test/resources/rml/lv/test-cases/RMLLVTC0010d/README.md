## RMLLVTC0010d

**Title**: Expression Map in Joined Field: Template

**Description**: Test a template-valued expression map in joined field 

**Error expected?** No

**Input**
```
{
  "people": [
    {
      "firstName": "alice",
      "lastName": "smith",
      "items": [
        "sword",
        "shield"
      ]
    },
    {
      "firstName": "bob",
      "lastName": "jones",
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
    rml:fieldName "json_first_name";
    rml:reference "$.firstName";
  ];
  rml:field [
    a rml:ExpressionField;
    rml:fieldName "json_last_name";
    rml:reference "$.lastName";
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
  rml:leftJoin [
    rml:parentLogicalView :jsonView;
    rml:joinCondition [
      rml:parent "json_first_name";
      rml:child "csv_name";
    ];
    rml:field [
      a rml:ExpressionField;
      rml:fieldName "json_full_name";
      rml:template "{json_first_name} {json_last_name}";
    ];
  ].

:triplesMapPerson a rml:TriplesMap;
  rml:logicalSource :csvView;
  rml:subjectMap [ rml:template "http://example.org/person/{json_full_name}" ];
  rml:predicateObjectMap [
    rml:predicate :hasBirthYear;
    rml:objectMap [
      rml:reference "csv_birthyear";
      rml:datatype xsd:gYear;
    ];
  ].


```

**Output**
```
<http://example.org/person/bob%20jones> <http://example.org/hasBirthYear> "1999"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://example.org/person/alice%20smith> <http://example.org/hasBirthYear> "1995"^^<http://www.w3.org/2001/XMLSchema#gYear> .

```

