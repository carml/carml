## RMLTC-CC-0005-App2

**Title**: Append values of multiple term maps

**Description**: Tests appending of values from multiple term maps in an iteration. In this example, one of the arrays is empty and the expected behavior is to concatinate a list with the empty list.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-App2/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0005-App2/Friends.json)

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
_:n1d9b8ef217cb44f299a062d746116deab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab2 .
_:n1d9b8ef217cb44f299a062d746116deab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
_:n1d9b8ef217cb44f299a062d746116deab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab5 .
_:n1d9b8ef217cb44f299a062d746116deab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab4 .
_:n1d9b8ef217cb44f299a062d746116deab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "5" .
_:n1d9b8ef217cb44f299a062d746116deab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n1d9b8ef217cb44f299a062d746116deab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "6" .
_:n1d9b8ef217cb44f299a062d746116deab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:n1d9b8ef217cb44f299a062d746116deab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:n1d9b8ef217cb44f299a062d746116deab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .
_:n1d9b8ef217cb44f299a062d746116deab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:n1d9b8ef217cb44f299a062d746116deab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab3 .
_:n1d9b8ef217cb44f299a062d746116deab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
<http://example.com/base/e/a> <http://example.com/ns#with> _:n1d9b8ef217cb44f299a062d746116deab1 .
<http://example.com/base/e/b> <http://example.com/ns#with> _:n1d9b8ef217cb44f299a062d746116deab6 .
_:n1d9b8ef217cb44f299a062d746116deab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab8 .
_:n1d9b8ef217cb44f299a062d746116deab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n1d9b8ef217cb44f299a062d746116deab7 .
_:n1d9b8ef217cb44f299a062d746116deab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
```

