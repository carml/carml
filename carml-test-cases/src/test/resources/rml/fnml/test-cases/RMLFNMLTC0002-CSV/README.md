## RMLFNMLTC0002-CSV

**Title**: Function on object, 1 reference parameter

**Description**: Tests:
(1) if a function with one parameter can be used, (FnO)
(2) a reference parameter can be used (Term)

**Error expected?** No

**Input**
```
Id,Name,Comment,Class
1,Venus,A&B,A

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ex: <http://example.com/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rml: <http://w3id.org/rml/> .
@prefix fno: <https://w3id.org/function/ontology#> .
@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .

@base <http://example.com/base/> .

<TriplesMap1>
    rml:logicalSource [
        rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "student.csv"
        ];
        rml:referenceFormulation rml:CSV
    ];
    rml:subjectMap [
        rml:template "http://example.com/{Name}"
    ];
    rml:predicateObjectMap [
        rml:predicate foaf:name;
        rml:objectMap [
            rml:functionExecution <#Execution> ;
            rml:return grel:stringOut
        ]
    ] .

<#Execution>
    rml:function grel:toUpperCase ;
    rml:input
        [
            rml:parameter grel:valueParam ;
            rml:inputValueMap [
                rml:reference "Name" ;
            ]
        ] .

```

**Output**
```
<http://example.com/Venus> <http://xmlns.com/foaf/0.1/name> "VENUS" .

```

