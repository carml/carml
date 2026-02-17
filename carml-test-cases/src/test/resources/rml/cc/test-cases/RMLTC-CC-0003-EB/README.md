## RMLTC-CC-0003-EB

**Title**: Allow the generation of empty bags

**Description**: Tests if the use of rml:allowEmptyListAndContainer yields an empty bag.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-EB/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-EB/Friends.json)

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
            rml:allowEmptyListAndContainer true ;
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
_:nfecac3cfe48c4afa99539cc809138e96b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
<http://example.com/base/e/a> <http://example.com/ns#with> _:nfecac3cfe48c4afa99539cc809138e96b1 .
_:nfecac3cfe48c4afa99539cc809138e96b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:nfecac3cfe48c4afa99539cc809138e96b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
<http://example.com/base/e/b> <http://example.com/ns#with> _:nfecac3cfe48c4afa99539cc809138e96b2 .
_:nfecac3cfe48c4afa99539cc809138e96b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
_:nfecac3cfe48c4afa99539cc809138e96b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
```

