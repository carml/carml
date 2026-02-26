## RMLFNMLTC0031-CSV

**Title**: Function on subject, 1 parameter

**Description**: Tests if 
(1) a function can be used on a subject
(2) Tests if the default termType assigned to the output of the function to be correct

**Error expected?** No

**Input**
```
Id,Name,url
1,Venus,www.example.com

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
        rml:functionExecution <#Execution> ;
        rml:return idlab-fn:_stringOut
    ];
    rml:predicateObjectMap [
        rml:predicate foaf:name;
        rml:objectMap [ rml:reference "Name"]
    ] .

<#Execution>
    rml:function idlab-fn:toUpperCaseURL ;
    rml:input
        [
            rml:parameter idlab-fn:str ;
            rml:inputValueMap [
                rml:reference "url"
            ];
        ] .

```

**Output**
```
<http://WWW.EXAMPLE.COM> <http://xmlns.com/foaf/0.1/name> "Venus" .

```

