## RMLTC-CC-0003-NEB

**Title**: Empty bags are not generated

**Description**: Tests if empty containers are not generated (expected default behavior).

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-NEB/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-NEB/Friends.json)

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
            rml:gather ( [ rml:reference "$.values.*" ; ] ) ;
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
<http://example.com/base/e/a> <http://example.com/ns#with> _:n2bd72de9c571444081890e41009d92d4b1 .
_:n2bd72de9c571444081890e41009d92d4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
_:n2bd72de9c571444081890e41009d92d4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:n2bd72de9c571444081890e41009d92d4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
_:n2bd72de9c571444081890e41009d92d4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
```

