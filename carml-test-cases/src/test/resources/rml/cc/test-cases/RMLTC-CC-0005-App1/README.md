## RMLTC-CC-0005-App1

**Title**: Append values of multiple term maps

**Description**: Tests appending of values from multiple term maps in an gather map. This test covers the case where all sets of values are non-empty.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-App1/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-App1/Friends.json)

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
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
<http://example.com/base/e/a> <http://example.com/ns#with> _:n1ee4a5a2b1344422919d171ab2cd6c39b1 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1ee4a5a2b1344422919d171ab2cd6c39b2 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1ee4a5a2b1344422919d171ab2cd6c39b4 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1ee4a5a2b1344422919d171ab2cd6c39b5 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
<http://example.com/base/e/b> <http://example.com/ns#with> _:n1ee4a5a2b1344422919d171ab2cd6c39b6 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1ee4a5a2b1344422919d171ab2cd6c39b7 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1ee4a5a2b1344422919d171ab2cd6c39b8 .
_:n1ee4a5a2b1344422919d171ab2cd6c39b8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "c" .
_:n1ee4a5a2b1344422919d171ab2cd6c39b8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

```

