## RMLTC-CC-0001-List

**Title**: Generate a rdf:List as an object

**Description**: Tests if a rdf:List is generated as an object

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0001-List/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0001-List/Friends.json)

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
<http://example.com/base/e/a> <http://example.com/ns#with> _:n988e0f11d19c486fa3b3255f94c9b3bab1 .
_:n988e0f11d19c486fa3b3255f94c9b3bab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
_:n988e0f11d19c486fa3b3255f94c9b3bab1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab2 .
_:n988e0f11d19c486fa3b3255f94c9b3bab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:n988e0f11d19c486fa3b3255f94c9b3bab2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab3 .
_:n988e0f11d19c486fa3b3255f94c9b3bab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
_:n988e0f11d19c486fa3b3255f94c9b3bab3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/b> <http://example.com/ns#with> _:n988e0f11d19c486fa3b3255f94c9b3bab4 .
_:n988e0f11d19c486fa3b3255f94c9b3bab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
_:n988e0f11d19c486fa3b3255f94c9b3bab4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab5 .
_:n988e0f11d19c486fa3b3255f94c9b3bab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "5" .
_:n988e0f11d19c486fa3b3255f94c9b3bab5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab6 .
_:n988e0f11d19c486fa3b3255f94c9b3bab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "6" .
_:n988e0f11d19c486fa3b3255f94c9b3bab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

<http://example.com/base/e/c> <http://example.com/ns#with> _:n988e0f11d19c486fa3b3255f94c9b3bab7 .
_:n988e0f11d19c486fa3b3255f94c9b3bab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "7" .
_:n988e0f11d19c486fa3b3255f94c9b3bab7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab8 .
_:n988e0f11d19c486fa3b3255f94c9b3bab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "8" .
_:n988e0f11d19c486fa3b3255f94c9b3bab8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n988e0f11d19c486fa3b3255f94c9b3bab9 .
_:n988e0f11d19c486fa3b3255f94c9b3bab9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "9" .
_:n988e0f11d19c486fa3b3255f94c9b3bab9 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .

```

