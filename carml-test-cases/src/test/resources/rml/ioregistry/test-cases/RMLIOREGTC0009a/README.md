## RMLIOREGTC0009a

**Title**: Access a JSON over Kafka

**Description**: Access a Kafka stream with JSON data

**Error expected?** No

**Input**
```
{
  "students": [{
    "Name":"Venus"
  }]
}

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .
@prefix td: <https://www.w3.org/2019/wot/td#>;
@prefix hctl: <https://www.w3.org/2019/wot/hypermedia#>;
@prefix kafka: <http://example.org/kafka/>;

<#KafkaStream> a rml:LogicalSource;
    rml:source [ a rml:Source, td:Thing;
        td:hasPropertyAffordance [
            td:hasForm [
                # URL and content type
                hctl:hasTarget "kafka://localhost/topic";
                hctl:forContentType "application/json";
                # Set Kafka parameters through W3C WoT Binding Template for Kafka
		kafka:groupId "MyAwesomeGroup";
            ];
        ];
    ];
    rml:referenceFormulation rml:JSONPath;
    rml:iterator "$.students[*]";
.

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource <#KafkaStream>;
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "$.Name"
        ];
      rml:predicate foaf:name
    ];
  rml:subjectMap <http://example.com/base/#NameSubjectMap> .

<http://example.com/base/#NameSubjectMap> rml:template "http://example.com/{$.Name}" .

```

**Output**
```
<http://example.com/Venus> <http://xmlns.com/foaf/0.1/name> "Venus" .


```

