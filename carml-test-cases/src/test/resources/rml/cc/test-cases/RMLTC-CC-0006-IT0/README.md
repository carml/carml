## RMLTC-CC-0006-IT0

**Title**: Gather values across iterations to create a collection.

**Description**: When no template, constant, or reference is given to a gather map, then each iteration yields a different collection.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT0/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT0/Friends.json)

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
<http://example.com/base/e/a> <http://example.com/ns#with> _:n9d785cd672d3484cb5d9fd831e6dfc07b1 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n9d785cd672d3484cb5d9fd831e6dfc07b2 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> _:n9d785cd672d3484cb5d9fd831e6dfc07b5 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n9d785cd672d3484cb5d9fd831e6dfc07b6 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/a> <http://example.com/ns#with> _:n9d785cd672d3484cb5d9fd831e6dfc07b3 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "5" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n9d785cd672d3484cb5d9fd831e6dfc07b4 .
_:n9d785cd672d3484cb5d9fd831e6dfc07b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "6" .
_:n9d785cd672d3484cb5d9fd831e6dfc07b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
```

