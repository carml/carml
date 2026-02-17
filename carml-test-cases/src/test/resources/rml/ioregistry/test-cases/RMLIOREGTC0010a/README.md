## RMLIOREGTC0010a

**Title**: Access a JSON over MQTT

**Description**: Access a MQTT stream with JSON data

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
@prefix mqv: <http://example.org/mqv/>;

<#MQTTStream> a rml:LogicalSource;
    rml:source [ a rml:Source, td:Thing;
        td:hasPropertyAffordance [
            td:hasForm [
                # URL and content type
                hctl:hasTarget "mqtt://localhost/topic";
                hctl:forContentType "application/json";
                # Set MQTT parameters through W3C WoT Binding Template for MQTT
                mqv:controlPacketValue "SUBSCRIBE";
                mqv:options ([ mqv:optionName "qos"; mqv:optionValue "1" ] [ mqv:optionName "dup" ]);
            ];
        ];
    ];
    rml:referenceFormulation rml:JSONPath;
    rml:iterator "$.students[*]";
.

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource <#MQTTStream>;
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

