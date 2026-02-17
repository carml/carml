## RMLTC-CC-0002-Bag

**Title**: Generate a named rdf:Bag as an object

**Description**: Tests if a named rdf:Bag is generated as an object

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0002-Bag/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0002-Bag/Friends.json)

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
            rml:gather ( [ rml:reference "$.values.*" ; ] ) ;
            rml:gatherAs rdf:Bag ;
        ] ;
    ] ;
.

_:b738439 a rml:RelativePathSource ;
    rml:root rml:MappingDirectory ;
    rml:path "data.json" .
```

**Output**
```
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "7" .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "8" .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "2" .
<http://example.com/base/e/c> <http://example.com/ns#with> <http://example.com/base/c/c> .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "6" .
<http://example.com/base/e/a> <http://example.com/ns#with> <http://example.com/base/c/a> .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "9" .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "1" .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> "3" .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> "4" .
<http://example.com/base/e/b> <http://example.com/ns#with> <http://example.com/base/c/b> .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> "5" .
```

