## RMLTC-CC-0010-Listb

**Title**: Combining graph maps and gather maps (both graph map and gather map have a template)

**Description**: Tests the behavior of combining graph- and gather maps. In this example, lists are concatinated in some named graphs via blank node identifiers constructed with templates.

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0010-Listb/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0010-Listb/Friends.json)

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
        rml:graphMap [ rml:template "g/{$.graph}" ; ] ;
        rml:predicate ex:with ;
        rml:objectMap [
            rml:template "l/{$.id}" ; rml:termType rml:BlankNode ;
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
_:genid1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" <http://example.com/base/g/1> .
_:genid1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:genid2 <http://example.com/base/g/1> .
_:genid2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" <http://example.com/base/g/1> .
_:genid2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> <http://example.com/base/g/1> .
<http://example.com/base/e/a> <http://example.com/ns#with> _:genid1 <http://example.com/base/g/1> .

_:genidx1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" <http://example.com/base/g/2> .
_:genidx1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> <http://example.com/base/g/2> .
<http://example.com/base/e/a> <http://example.com/ns#with> _:genidx1 <http://example.com/base/g/2> .
```

