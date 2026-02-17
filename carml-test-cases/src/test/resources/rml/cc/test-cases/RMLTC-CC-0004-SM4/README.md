## RMLTC-CC-0004-SM4

**Title**: GatherMap in subject map (bag)

**Description**: Tests if the use of a gather map in the subject map is supported. This test generates a bag.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM4/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM4/Friends.json)

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
        rml:gather ( [ rml:reference "$.values.*" ; ] ) ;
        rml:gatherAs rdf:Bag ;
    ] ;

    rml:predicateObjectMap [
        rml:predicate ex:createdBy ;
        rml:object ex:JohnDoe ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
_:n1d393f12e993411498da676131f81ca4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
_:n1d393f12e993411498da676131f81ca4b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "5" .
_:n1d393f12e993411498da676131f81ca4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
_:n1d393f12e993411498da676131f81ca4b2 <http://example.com/ns#createdBy> <http://example.com/ns#JohnDoe> .
_:n1d393f12e993411498da676131f81ca4b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:n1d393f12e993411498da676131f81ca4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
_:n1d393f12e993411498da676131f81ca4b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "4" .
_:n1d393f12e993411498da676131f81ca4b1 <http://example.com/ns#createdBy> <http://example.com/ns#JohnDoe> .
_:n1d393f12e993411498da676131f81ca4b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:n1d393f12e993411498da676131f81ca4b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "6" .
```

