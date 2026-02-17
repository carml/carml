## RMLTC-CC-0001-Seq

**Title**: Generate a rdf:Seq as an object

**Description**: Tests if a rdf:Seq is generated as an object

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0001-Seq/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0001-Seq/Friends.json)

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
            rml:gatherAs rdf:Seq ;
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
_:nc808fbd2e11a4baa939c61c8210b5909b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> .
_:nc808fbd2e11a4baa939c61c8210b5909b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "5" .
_:nc808fbd2e11a4baa939c61c8210b5909b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> .
_:nc808fbd2e11a4baa939c61c8210b5909b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> .
_:nc808fbd2e11a4baa939c61c8210b5909b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "9" .
_:nc808fbd2e11a4baa939c61c8210b5909b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
<http://example.com/base/e/b> <http://example.com/ns#with> _:nc808fbd2e11a4baa939c61c8210b5909b2 .
_:nc808fbd2e11a4baa939c61c8210b5909b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
<http://example.com/base/e/c> <http://example.com/ns#with> _:nc808fbd2e11a4baa939c61c8210b5909b3 .
<http://example.com/base/e/a> <http://example.com/ns#with> _:nc808fbd2e11a4baa939c61c8210b5909b1 .
_:nc808fbd2e11a4baa939c61c8210b5909b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "4" .
_:nc808fbd2e11a4baa939c61c8210b5909b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "7" .
_:nc808fbd2e11a4baa939c61c8210b5909b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "6" .
_:nc808fbd2e11a4baa939c61c8210b5909b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
_:nc808fbd2e11a4baa939c61c8210b5909b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "8" .
```

