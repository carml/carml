## RMLIOREGTC0003b

**Title**: Missing XML value

**Description**: Handle missing XML value in rml:reference

**Error expected?** Yes

**Input**
```
<?xml version="1.0"?>

<students>
  <student>
    <Name>Venus</Name>
  </student>
</students>

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator "/students/student";
      rml:referenceFormulation rml:XPath;
      rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "student.xml"
        ]
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "NON_EXISTING"
        ];
      rml:predicate foaf:name
    ];
  rml:subjectMap [
      rml:template "http://example.com/{Name}"
    ] .

```

