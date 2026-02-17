## RMLIOREGTC0003e

**Title**: XML attributes selection

**Description**: Test generation of triples with selected XML attributes

**Error expected?** No

**Input**
```
<?xml version="1.0"?>

<persons>
  <person fname="Bob" lname="Smith" amount="30.0E0">
  </person>
  <person fname="Sue" lname="Jones" amount="20.0E0">
  </person>
  <person fname="Bob" lname="Smith" amount="30.0E0">
  </person>
</persons>

```

**Mapping**
```
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap ;
    rml:logicalSource [ a rml:LogicalSource ;
            rml:iterator "/persons/person" ;
            rml:referenceFormulation rml:XPath ;
            rml:source _:b347486 ] ;
    rml:predicateObjectMap [ rml:objectMap [ rml:reference "@amount" ] ;
            rml:predicate ex:owes ] ;
    rml:subjectMap [ rml:class foaf:Person ;
            rml:template "http://example.com/{@fname};{@lname}" ] .

_:b347486 a rml:RelativePathSource ;
    rml:path "student.xml" .

```

**Output**
```
<http://example.com/Bob;Smith> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Bob;Smith> <http://example.com/owes> "30.0E0" .
<http://example.com/Sue;Jones> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/Sue;Jones> <http://example.com/owes> "20.0E0" .

```

