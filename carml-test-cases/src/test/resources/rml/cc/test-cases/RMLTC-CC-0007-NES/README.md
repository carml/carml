## RMLTC-CC-0007-NES

**Title**: Nested gather maps

**Description**: Testing whether nested gather maps are created. The mapping should generate a list of bags.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0007-NES/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0007-NES/Friends.json)

**Mapping**
```
@prefix rml: <http://w3id.org/rml/>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix ex:  <http://example.com/ns#>.

<http://example.com/base#TM> a rml:TriplesMap;
    rml:logicalSource [
        rml:source _:b738439 ;
        rml:referenceFormulation rml:JSONPath ;
        rml:iterator "$.*" ;
    ] ;

    rml:subjectMap [
        rml:template "e/{$.id}" ;
    ] ;

    rml:predicateObjectMap [
        rml:predicate ex:with ;
        rml:objectMap [
            rml:gather (
                [
                    rml:gather ( [ rml:reference "$.v1.*" ; ] ) ;
                    rml:gatherAs rdf:Bag ;
                ]
                [
                    rml:gather ( [ rml:reference "$.v2.*" ; ] ) ;
                    rml:gatherAs rdf:Bag ;
                ]
            ) ;
            rml:gatherAs rdf:List ;
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
_:nab9af1206e584ab2afd457e8a605872cb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:nab9af1206e584ab2afd457e8a605872cb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
_:nab9af1206e584ab2afd457e8a605872cb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .

_:nab9af1206e584ab2afd457e8a605872cb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:nab9af1206e584ab2afd457e8a605872cb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "a" .
_:nab9af1206e584ab2afd457e8a605872cb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "b" .

_:nab9af1206e584ab2afd457e8a605872cb5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:nab9af1206e584ab2afd457e8a605872cb5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "3" .
_:nab9af1206e584ab2afd457e8a605872cb5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "4" .

_:nab9af1206e584ab2afd457e8a605872cb6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:nab9af1206e584ab2afd457e8a605872cb6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "c" .

<http://example.com/base/e/a> <http://example.com/ns#with> _:nab9af1206e584ab2afd457e8a605872cb3 .
_:nab9af1206e584ab2afd457e8a605872cb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:nab9af1206e584ab2afd457e8a605872cb1 .
_:nab9af1206e584ab2afd457e8a605872cb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nab9af1206e584ab2afd457e8a605872cb4 .

_:nab9af1206e584ab2afd457e8a605872cb4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:nab9af1206e584ab2afd457e8a605872cb2 .
_:nab9af1206e584ab2afd457e8a605872cb4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> _:nab9af1206e584ab2afd457e8a605872cb7 .
_:nab9af1206e584ab2afd457e8a605872cb7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:nab9af1206e584ab2afd457e8a605872cb5 .
_:nab9af1206e584ab2afd457e8a605872cb7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nab9af1206e584ab2afd457e8a605872cb8 .

_:nab9af1206e584ab2afd457e8a605872cb8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:nab9af1206e584ab2afd457e8a605872cb6 .
_:nab9af1206e584ab2afd457e8a605872cb8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .


```

