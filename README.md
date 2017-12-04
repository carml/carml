<p align="center">
<img src="https://raw.githubusercontent.com/carml/carml.github.io/master/carml-logo.png" height="100" alt="carml">
</p>

CARML
=====================
**A pretty sweet RML engine**

(**Disclaimer:** The current state of CARML is early beta.
The team expects to deliver a release in November 2017.
The next release will offer improved code quality, more test coverage, more documentation and several features currently on the product backlog.)

CARML is being developed by [Taxonic](http://www.taxonic.com) in cooperation with [Kadaster](https://www.kadaster.com/).

[![Build Status](https://api.travis-ci.org/carml/carml.svg?branch=master)](https://travis-ci.org/carml/carml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.taxonic.carml/carml/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.taxonic.carml/carml)

Table of Contents
-----------------
- [Releases](#releases)
- [Introduction](#introduction)
- [Getting started](#getting-started)
- [Validating your RML mapping](#validating-your-rml-mapping)
- [Input stream extension](#input-stream-extension)
- [Function extension](#function-extension)
- [Supported data source types](#supported-data-source-types)
- [Roadmap](#roadmap)

Releases
----
20 Sep 2017 - CARML 0.0.1

21 Oct 2017 - CARML 0.1.0

Introduction
------------
CARML is a java library that transforms structured sources to RDF based as declared in and [RML](http://rml.io) mapping, in accordance with the [RML spec](http://rml.io/spec.html). It is considered by many as the optimal choice for mapping structured sources to RDF.

The best place to start learning about RML is at the [source](http://rml.io), but basically
RML is defined as a superset of [R2RML](https://www.w3.org/TR/r2rml/) which is a W3C recommendation that describes a language for expressing mappings from relational databases to RDF datasets. RML allows not only the expression of mappings for relational databases, but generalizes this to any structured source. All you need is a way to iterate over and query the source.

Getting started
---------------

CARML is available from the Central Maven Repository.

```xml
<dependency>
    <groupId>com.taxonic.carml</groupId>
    <artifactId>carml-engine</artifactId>
    <version>0.1.0</version>
</dependency>
```

CARML is based on [RDF4J](http://rdf4j.org/), and currently the Mapper directly outputs an [RDF4J Model](http://docs.rdf4j.org/javadoc/2.0/org/eclipse/rdf4j/model/package-summary.html).

```java
Set<TriplesMap> mapping =
	RmlMappingLoader
		.build()
		.load(Paths.get("path-to-mapping-file"), RDFFormat.TURTLE);

RmlMapper mapper =
	RmlMapper
		.newBuilder()
		.fileResolver(Paths.get("folder/containing/data/sources"))
		.addFunctions(new RmlFunctions())
		.build();

Model result = mapper.map(mapping);
```

Validating your RML mapping
---------------------------
We're not set up for full mapping validation yet. But, to help you get those first nasty mapping errors out, we've created a [SHACL](https://www.w3.org/TR/shacl/) shapes graph ([here](https://github.com/carml/carml/tree/master/rml.sh.ttl)) that validates RML mappings. You can use the [SHACL playground](http://shacl.org/playground/) to easily test your mapping.

Input stream extension
---------------------
When it comes to non-database sources, the current RML spec only supports the specification of file based sources in an `rml:LogicalSource`. However, it is often very useful to be able to transform stream sources.

To this end CARML introduces the notion of 'Named Streams'.
Which follows the ontology defined [here](https://github.com/carml/carml/tree/master/carml.ttl).

So now, you can define streams in your mapping like so:
```
:SomeLogicalSource
  rml:source [
    a carml:Stream ;
    # NOTE: name is not mandatory and can be left unspecified, when working with a single stream
    carml:streamName "stream-A" ;
  ];
  rml:referenceFormulation ql:JSONPath;
  rml:iterator "$"
.
```
In order to provide access to the input stream, it needs to be registered on the mapper.
```java
RmlMapper mapper = RmlMapper.newBuilder().build();
mapper.bindInputStream("stream-A", inputStream);
```
Note that it is possible to register several streams, allowing you to combine several streams to create your desired RDF output.

Function extension
------------------
A recent development related to RML is the possibility of adding functions to the mix. This is a powerful extension that increases the expressivity of mappings immensely. Do note that the function ontology is still under development at UGhent.

Because we believe that this extension can already be of great value, we've already adopted it in CARML.

<!--- TODO: explain that the function execution is a finisher, that is it runs the normal mapping, which creates the function execution triples, and the described execution is in turn evaluated and results in the term map value. --->
The way it works is, you describe the execution of a function in terms of the [Function Ontology (FnO)](https://fno.io/).

Take for example the SumFunction example of the [FnO spec](http://users.ugent.be/~bjdmeest/function/#complete-example). This defines an instance `ex:sumFunction` of class `fno:Function` that is able to compute the sum of two values provided as parameters of the function at execution time.

It also describes an instance `ex:sumExecution` of `fno:execution`, which `fno:executes` `ex:sumFunction` which descibes an instance of an execution of the defined sum function. In this case with parameters 2 and 4.

To be able to use this in RML mappings we use executions of instances of `fno:Function` to determine the value of a term map. The execution of a function can be seen as a post-processing step in the evaluation of a term map.

```
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix fnml:   <http://semweb.mmlab.be/ns/fnml#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix fno: <http://semweb.datasciencelab.be/ns/function#> .
@prefix ex: <http://example.org#> .

ex:sumValuePredicateObjectMap
  rr:predicate ex:total ;
  rr:objectMap [
    a fnml:FunctionTermMap ;
    fnml:functionValue [
      rml:logicalSource ex:LogicalSource ;
      rr:subjectMap [
        rr:template "functionExec";
        rr:termType rr:BlankNode ;
        rr:class fno:Execution
      ] ;
      rr:predicateObjectMap
        [
          rr:predicate fno:executes ;
          rr:objectMap [
            rr:constant ex:sumFunction ;
          ]
        ] ,
        [
          rr:predicate ex:intParameterA ;
          rr:objectMap [ rml:reference "foo" ]
        ] ,
        [
          rr:predicate ex:intParameterB  ;
          rr:objectMap [ rml:reference "bar" ]
        ]
    ] ;
    rr:datatype xsd:integer ;
  ]
.
```

A function can be created in any `.java` class. The function should be annotated with `@FnoFunction`, providing the string value of the function IRI, and the function parameters with `@FnoParam`, providing the string value of the function parameter IRIs.

```java
public class RmlFunctions {

	@FnoFunction("http://example.org/sumFunction")
	public int sumFunction(
		@FnoParam("http://example.org/intParameterA") int intA,
		@FnoParam("http://example.org/intParameterB") int intB
  	) {
  		return intA + intB;
  	}

}
```

The class or classes containing the annotated functions can then be registered on the mapper via the `RmlMapper#addFunctions` method.

```java
RmlMapper mapper =
	RmlMapper
		.newBuilder()
		.classPathResolver(contextPath)
		.addFunctions(new RmlFunctions())
		.build();
Model result = mapper.map(mapping);
```

It is recommended to describe and publish new functions in terms of FnO for interpretability of mappings, and, possibly, reuse of functions, but it's not mandatory for use in CARML.

Note that it is currently possible to specify and use function executions as parameters of other function executions in CARML, although this is not (yet?) expressible in FnO.


Supported Data Source Types
---------------------------

| Data source type          | Reference query language                                       |
| :------------------------ | :------------------------------------------------------------- |
| JSON                      | [Jayway JsonPath 2.4.0](https://github.com/json-path/JsonPath) |

Coming soon:
XML, CSV

Roadmap
-------
* Add support for XML sources
* Add support for CSV sources
* CARML Command line interface
* Better support for large sources
* Improved join / parent triples map performance
* Split off provisional RDF Mapper as separate library
