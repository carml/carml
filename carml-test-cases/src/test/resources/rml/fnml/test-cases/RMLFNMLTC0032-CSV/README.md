## RMLFNMLTC0032-CSV

**Title**: Condition on subject

**Description**: Tests that a condition shortcut works

**Error expected?** No

**Input**
```
Id,Name,Class
1,Venus,A
1,Serena,B

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ex: <http://example.com/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rml: <http://w3id.org/rml/> .
@prefix fno: <https://w3id.org/function/ontology#> .
@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#> .

@base <http://example.com/base/> .

<TriplesMap1>
    rml:logicalSource [
        rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "student.csv"
        ];
        rml:referenceFormulation rml:CSV
    ] ;
    rml:subjectMap [
        rml:condition [
            rml:functionExecution [
                rml:function idlab-fn:equal ;
                rml:input [
                    rml:parameter grel:valueParam ;
                    rml:inputValueMap [
                        rml:reference "Class"
                    ]
                ] , [
                    rml:parameter grel:valueParam2 ;
                    rml:inputValue "A" ;
                ] ;
            ] ;
            rml:return idlab-fn:_boolOut ;
        ] ;
        rml:template "http://example.com/{Name}" ;
    ] ;
    rml:predicateObjectMap [
        rml:predicate foaf:name ;
        rml:objectMap [
            rml:reference "Name" ;
        ] ;
    ] ;
.

```

**Output**
```
<http://example.com/Venus> <http://xmlns.com/foaf/0.1/name> "Venus" .

```

