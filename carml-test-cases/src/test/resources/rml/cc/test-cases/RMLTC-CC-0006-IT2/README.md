## RMLTC-CC-0006-IT2

**Title**: Gather values across iterations to create a collection.

**Description**: When using a template, constant, or reference for a gather map, this tests determines whether the values are correctly appended to the list. The natural order of the term maps inside the gather map as well as the iteration are respected. This test covers two term maps in the gather map.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT2/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0006-IT2/Friends.json)

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
            rml:template "c/{$.id}" ;
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
<http://example.com/base/e/a> <http://example.com/ns#with> <http://example.com/base/c/a> .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab1 .
_:n0cbb21ce98464358963007f95dc35c0ab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n0cbb21ce98464358963007f95dc35c0ab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab2 .
_:n0cbb21ce98464358963007f95dc35c0ab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .
_:n0cbb21ce98464358963007f95dc35c0ab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab3 .
_:n0cbb21ce98464358963007f95dc35c0ab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
_:n0cbb21ce98464358963007f95dc35c0ab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab4 .
_:n0cbb21ce98464358963007f95dc35c0ab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "5" .
_:n0cbb21ce98464358963007f95dc35c0ab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab5 .
_:n0cbb21ce98464358963007f95dc35c0ab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "6" .
_:n0cbb21ce98464358963007f95dc35c0ab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab6 .
_:n0cbb21ce98464358963007f95dc35c0ab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "d" .
_:n0cbb21ce98464358963007f95dc35c0ab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab7 .
_:n0cbb21ce98464358963007f95dc35c0ab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "e" .
_:n0cbb21ce98464358963007f95dc35c0ab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> <http://example.com/base/c/b> .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab8 .
_:n0cbb21ce98464358963007f95dc35c0ab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
_:n0cbb21ce98464358963007f95dc35c0ab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n0cbb21ce98464358963007f95dc35c0ab9 .
_:n0cbb21ce98464358963007f95dc35c0ab9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "c" .
_:n0cbb21ce98464358963007f95dc35c0ab9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

```

