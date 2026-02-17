## RMLTC-CC-0004-SM3

**Title**: GatherMap in subject map (list)

**Description**: Tests if the use of a gather map in the subject map is supported. This test has no predicate-object maps and no class declarations in the subject map. Only one list needs to be generated as the other has no values and we only retain non-empty lists. 

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM3/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM3/Friends.json)

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
        rml:gatherAs rdf:List ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
_:n648f8cf4c58149e8a0feea26fb2a5b5bb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:n648f8cf4c58149e8a0feea26fb2a5b5bb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n648f8cf4c58149e8a0feea26fb2a5b5bb2 .
_:n648f8cf4c58149e8a0feea26fb2a5b5bb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:n648f8cf4c58149e8a0feea26fb2a5b5bb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n648f8cf4c58149e8a0feea26fb2a5b5bb3 .
_:n648f8cf4c58149e8a0feea26fb2a5b5bb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n648f8cf4c58149e8a0feea26fb2a5b5bb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
```

