## RMLIOREGTC0011a

**Title**: Access a SPARQL triplestore

**Description**: Access a SPARQL triple story using a SPARQL query

**Error expected?** No

**Mapping**
```
@prefix rml: <http://w3id.org/rml/>.
@prefix sd: <http://www.w3.org/ns/sparql-service-description#>.
@prefix foaf: <http://xmlns.com/foaf/0.1/>.
@prefix formats: <http://www.w3.org/ns/formats/>.

<#SDSourceAccess> a rml:Source, sd:Service;
  sd:endpoint <http://dbpedia.org/sparql/>;
  sd:supportedLanguage sd:SPARQL11Query;
.

<#TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
    rml:source <#SDSourceAccess>;
    rml:referenceFormulation formats:SPARQL_Results_CSV;
    rml:iterator """
      PREFIX dbo: <http://dbpedia.org/ontology/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      SELECT DISTINCT ?actor ?name WHERE {
        ?tvshow rdf:type dbo:TelevisionShow .
        ?tvshow rdfs:label "The Big Bang Theory"@en .
        ?tvshow dbo:starring ?actor .
        ?actor foaf:name ?name .
      }
    """;
  ];
  rml:subjectMap [ a rml:SubjectMap;
    rml:reference "actor";
    rml:termType rml:IRI;
  ];
  rml:predicateObjectMap [ a rml:PredicateObjectMap;
    rml:predicateMap [ a rml:PredicateMap;
      rml:constant foaf:name;
    ];
    rml:objectMap [ a rml:ObjectMap;
      rml:reference "name";
    ];
  ];
.

```

**Output**
```
<http://dbpedia.org/resource/Kevin_Sussman> <http://xmlns.com/foaf/0.1/name> "Kevin Sussman" .
<http://dbpedia.org/resource/Jim_Parsons> <http://xmlns.com/foaf/0.1/name> "Jim Parsons" .
<http://dbpedia.org/resource/Kaley_Cuoco> <http://xmlns.com/foaf/0.1/name> "Kaley Cuoco" .
<http://dbpedia.org/resource/Sara_Gilbert> <http://xmlns.com/foaf/0.1/name> "Sara Gilbert" .
<http://dbpedia.org/resource/Kunal_Nayyar> <http://xmlns.com/foaf/0.1/name> "Kunal Nayyar" .
<http://dbpedia.org/resource/Laura_Spencer> <http://xmlns.com/foaf/0.1/name> "Laura Spencer" .
<http://dbpedia.org/resource/Simon_Helberg> <http://xmlns.com/foaf/0.1/name> "Simon Helberg" .
<http://dbpedia.org/resource/Johnny_Galecki> <http://xmlns.com/foaf/0.1/name> "Johnny Galecki" .
<http://dbpedia.org/resource/Mayim_Bialik> <http://xmlns.com/foaf/0.1/name> "Mayim Bialik" .
<http://dbpedia.org/resource/Melissa_Rauch> <http://xmlns.com/foaf/0.1/name> "Melissa Rauch" .

```

