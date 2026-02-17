## RMLTC-CC-0004-SM5

**Title**: GatherMap in subject map (bag)

**Description**: Tests if the use of a gather map in the subject map is supported. This test has no predicate-object maps and no class declarations in the subject map. It should generate a non-empty bag and an empty bag. The reason being that a container has at least one triple ; it's type (bag, seq, or alt).

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM5/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0004-SM5/Friends.json)

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
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
_:ne32066984b71404a953025d001cd3cdfb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
_:ne32066984b71404a953025d001cd3cdfb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
_:ne32066984b71404a953025d001cd3cdfb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
_:ne32066984b71404a953025d001cd3cdfb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
```

