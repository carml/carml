## RMLFNMLTC0051-CSV

**Title**: Nested function - Test A

**Description**: Tests if a composite function of form f(g(x1),x2) works (i.e., the inner function is only one argument of the outer function)

**Error expected?** No

**Input**
```
Id,Name,Comment,Class
1,M Venus,A&B,A

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

<#Person_Mapping>
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
    rml:predicateObjectMap <#NameMapping> .

<#NameMapping>
    rml:predicate foaf:name ;
    rml:objectMap [
        rml:functionExecution <#Execution> ;
        rml:return grel:stringOut
    ]; .

<#Execution> a rml:FunctionExecution ;
    rml:function grel:toUpperCase ;
    rml:input
        [
            a rml:Input ;
            rml:parameter grel:valueParam ;
            rml:inputValueMap [
                rml:functionExecution <#Execution2> ;
                rml:return grel:stringOut
            ]
        ] .

<#Execution2> a rml:Execution ;
    rml:function grel:string_replace ;
    rml:input
        [
            a rml:Input ;
            rml:parameter grel:valueParam ;
            rml:inputValueMap [
                rml:reference "Name"
            ]
        ] ,
        [
            a rml:Input ;
            rml:parameter grel:param_find ;
            rml:inputValue " "
        ] ,
        [
            a rml:Input ;
            rml:parameter grel:param_replace  ;
            rml:inputValue "-"
        ] .

```

**Output**
```
<http://example.com/M%20Venus> <http://xmlns.com/foaf/0.1/name> "M-VENUS" .

```

