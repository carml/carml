## RMLTC-CC-0002-List

**Title**: Generate a named rdf:List as an object

**Description**: Tests if a named rdf:List is generated as an object

**Error expected?** No

**Input**
 [http://w3id.org/rml/resources/rml-io/RMLTC-CC-0002-List/Friends.json](http://w3id.org/rml/resources/rml-io/RMLTC-CC-0002-List/Friends.json)

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
_:nb73dd869d6bb47a5b865ee632fe27453b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b2 .
<http://example.com/base/e/c> <http://example.com/ns#with> <http://example.com/base/c/c> .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "7" .
_:nb73dd869d6bb47a5b865ee632fe27453b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "8" .
_:nb73dd869d6bb47a5b865ee632fe27453b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b4 .
_:nb73dd869d6bb47a5b865ee632fe27453b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
<http://example.com/base/e/a> <http://example.com/ns#with> <http://example.com/base/c/a> .
_:nb73dd869d6bb47a5b865ee632fe27453b3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "5" .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b3 .
_:nb73dd869d6bb47a5b865ee632fe27453b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "6" .
_:nb73dd869d6bb47a5b865ee632fe27453b4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:nb73dd869d6bb47a5b865ee632fe27453b5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b6 .
_:nb73dd869d6bb47a5b865ee632fe27453b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:nb73dd869d6bb47a5b865ee632fe27453b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "2" .
_:nb73dd869d6bb47a5b865ee632fe27453b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "3" .
<http://example.com/base/c/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "4" .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b1 .
<http://example.com/base/c/c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nb73dd869d6bb47a5b865ee632fe27453b5 .
_:nb73dd869d6bb47a5b865ee632fe27453b6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "9" .
<http://example.com/base/e/b> <http://example.com/ns#with> <http://example.com/base/c/b> .
<http://example.com/base/c/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1" .
```

