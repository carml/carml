## RMLFNMLTC0081-CSV

**Title**: Function on languageMap, 1 parameter

**Description**: Tests that function on LanguageMap is handled

**Error expected?** No

**Input**
```
Id,Name,Comment,Class,Lang
1,Venus,A&B,A,en

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
        rml:predicate foaf:name;
        rml:objectMap [
           rml:reference "Name" ;
           rml:languageMap [
             rml:functionExecution <#Execution> ;
             rml:return grel:stringOut
           ] ;
        ]
    ] .

<#Execution>
    rml:function grel:string_substring ;
    rml:input
        [
            rml:parameter grel:valueParam ;
            rml:inputValueMap [
                rml:reference "Lang"
            ];
        ]  ,
        [
            a rml:Input ;
            rml:parameter grel:p_int_i_from ;
            rml:inputValue "0"
        ]  .

```

**Output**
```
<http://example.com/Venus> <http://xmlns.com/foaf/0.1/name> "Venus"@en .

```

