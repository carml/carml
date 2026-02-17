## RMLTC-CC-0003-NEL

**Title**: Empty lists are not generated

**Description**: Tests if empty lists are not generated (expected default behavior).

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-NEL/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0003-NEL/Friends.json)

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
_:n434c2c54e09e4a259604ba1188ce038fb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n434c2c54e09e4a259604ba1188ce038fb2 .
_:n434c2c54e09e4a259604ba1188ce038fb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:n434c2c54e09e4a259604ba1188ce038fb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
<http://example.com/base/e/a> <http://example.com/ns#with> _:n434c2c54e09e4a259604ba1188ce038fb1 .
_:n434c2c54e09e4a259604ba1188ce038fb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n434c2c54e09e4a259604ba1188ce038fb3 .
_:n434c2c54e09e4a259604ba1188ce038fb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:n434c2c54e09e4a259604ba1188ce038fb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
```

