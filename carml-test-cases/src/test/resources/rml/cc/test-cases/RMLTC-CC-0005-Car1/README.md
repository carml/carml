## RMLTC-CC-0005-Car1

**Title**: Cartesian product of values from multiple term maps of a gather map.

**Description**: Tests generating the Cartesian product of values from multiple term maps in a gather map. This test covers the case where all sets of values are non-empty.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-Car1/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-Car1/Friends.json)

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
            rml:gather ( [ rml:reference "$.v1.*" ; ] [ rml:reference "$.v2.*" ; ] ) ;
            rml:gatherAs rdf:List ;
            rml:strategy rml:cartesianProduct ;
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
<http://example.com/base/e/a> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb1 .
_:nfc398e5692654ca98dfb3fb5432d277cb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:nfc398e5692654ca98dfb3fb5432d277cb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb2 .
_:nfc398e5692654ca98dfb3fb5432d277cb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:nfc398e5692654ca98dfb3fb5432d277cb2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .

<http://example.com/base/e/a> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb3 .
_:nfc398e5692654ca98dfb3fb5432d277cb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:nfc398e5692654ca98dfb3fb5432d277cb3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb4 .
_:nfc398e5692654ca98dfb3fb5432d277cb4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
_:nfc398e5692654ca98dfb3fb5432d277cb4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/a> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb5 .
_:nfc398e5692654ca98dfb3fb5432d277cb5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:nfc398e5692654ca98dfb3fb5432d277cb5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb6 .
_:nfc398e5692654ca98dfb3fb5432d277cb6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .
_:nfc398e5692654ca98dfb3fb5432d277cb6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/a> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb7 .
_:nfc398e5692654ca98dfb3fb5432d277cb7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:nfc398e5692654ca98dfb3fb5432d277cb7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb8 .
_:nfc398e5692654ca98dfb3fb5432d277cb8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
_:nfc398e5692654ca98dfb3fb5432d277cb8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb9 .
_:nfc398e5692654ca98dfb3fb5432d277cb9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:nfc398e5692654ca98dfb3fb5432d277cb9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb10 .
_:nfc398e5692654ca98dfb3fb5432d277cb10 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "c" .
_:nfc398e5692654ca98dfb3fb5432d277cb10 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> _:nfc398e5692654ca98dfb3fb5432d277cb11 .
_:nfc398e5692654ca98dfb3fb5432d277cb11 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
_:nfc398e5692654ca98dfb3fb5432d277cb11 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nfc398e5692654ca98dfb3fb5432d277cb12 .
_:nfc398e5692654ca98dfb3fb5432d277cb12 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "c" .
_:nfc398e5692654ca98dfb3fb5432d277cb12 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
```

