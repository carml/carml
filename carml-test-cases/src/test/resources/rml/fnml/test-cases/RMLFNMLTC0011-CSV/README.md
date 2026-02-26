## RMLFNMLTC0011-CSV

**Title**: Function on predicate, 1 parameter

**Description**: Tests if a function can be used on a predicate

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
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#> .

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
        rml:predicateMap [
            rml:functionExecution <#Execution> ;
            rml:return idlab-fn:_stringOut
        ];
        rml:object foaf:name;
    ] .

<#Execution>
    rml:function idlab-fn:toUpperCaseURL ;
    rml:input
        [
            rml:parameter idlab-fn:str ;
            rml:inputValueMap [
                rml:reference "Name"
            ];
        ] .

```

**Output**
```
<http://example.com/Venus> <http://VENUS> <http://xmlns.com/foaf/0.1/name> .

```

