## RMLTC-CC-0006-IT3

**Title**: Gather values across iterations to create a container.

**Description**: When no template, constant, or reference is given to a gather map, then each iteration yields a different container.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT3/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT3/Friends.json)

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
            rml:gather ( [ rml:reference "$.v1.*" ; ] ) ;
            rml:gatherAs rdf:Bag ;
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
<http://example.com/base/e/a> <http://example.com/ns#with> _:a .
_:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
_:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .

<http://example.com/base/e/b> <http://example.com/ns#with> _:b .
_:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "3" .
_:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "4" .

<http://example.com/base/e/a> <http://example.com/ns#with> _:c .
_:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "5" .
_:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "6" .
```

