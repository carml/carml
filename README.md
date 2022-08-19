<p align="center">
<img src="https://raw.githubusercontent.com/carml/carml.github.io/master/carml-logo.png" height="100" alt="carml">
</p>

CARML
=====================
**A pretty sweet RML engine**

[![Build](https://github.com/carml/carml/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/carml/carml/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.carml/carml/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.carml/carml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=io.carml%3Acarml&metric=alert_status)](https://sonarcloud.io/dashboard?id=io.carml%3Acarml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=io.carml%3Acarml&metric=coverage)](https://sonarcloud.io/dashboard?id=io.carml%3Acarml)

Table of Contents
-----------------

- [Introduction](#introduction)
- [Getting started](#getting-started)
- [Reactive streams](#reactive-streams)
- [Support for Apache Jena](#support-for-apache-jena)
- [Input stream extension](#input-stream-extension)
- [Function extension](#function-extension)
- [XML namespace extension](#xml-namespace-extension)
- [CARML in RML Test Cases](#carml-in-rml-test-cases)
- [About CARML](#about-carml)

Introduction
------------
CARML is a java library that transforms structured sources to RDF as declared in an [RML](http://rml.io) mapping,
in accordance with the [RML spec](http://rml.io/spec.html).

The best place to start learning about RML is at the [source](http://rml.io), but basically RML is defined as a superset
of [R2RML](https://www.w3.org/TR/r2rml/) which is a W3C recommendation that describes a language for expressing mappings
from relational databases to RDF datasets. RML allows not only the expression of mappings for relational databases, but
generalizes this to any structured source. All you need is a way to iterate over and query the source.

> NOTE: If you have questions about RML in general, the best place to ask them is at
> [RML questions](https://github.com/kg-construct/rml-questions).

Getting started
---------------

CARML is available from the Central Maven Repository.

```xml

<dependency>
    <groupId>io.carml</groupId>
    <artifactId>carml-engine</artifactId>
    <version>${carml.version}</version>
</dependency>

    <!-- Choose the resolvers to suit your need -->
<dependency>
    <groupId>io.carml</groupId>
    <artifactId>carml-logical-source-resolver-jsonpath</artifactId>
    <version>${carml.version}</version>
    </dependency>
<dependency>
    <groupId>io.carml</groupId>
    <artifactId>carml-logical-source-resolver-xpath</artifactId>
    <version>${carml.version}</version>
</dependency>
<dependency>
    <groupId>io.carml</groupId>
    <artifactId>carml-logical-source-resolver-csv</artifactId>
    <version>${carml.version}</version>
</dependency>

```

Example usage:

```java
Set<TriplesMap> mapping = RmlMappingLoader.build()
    .load(RDFFormat.TURTLE, Paths.get("path-to-mapping-file"));

RdfRmlMapper mapper = RdfRmlMapper.builder()
    // add mappings
    .triplesMaps(mapping)
    // Add the resolvers to suit your need
    .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
    .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
    .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
    //-- optional: --
    // specify base IRI to use for relative IRIs in mapping results
    // default is "http://example.com/base/"
    .baseIri("http://foo.bar")
    // specify IRI unicode normalization form (default = NFC)
    // see http://www.unicode.org/unicode/reports/tr15/tr15-23.html
    .iriUnicodeNormalization(Form.NFKC)
    // set file directory for sources in mapping
    .fileResolver("/some/dir/")
    // set classpath basepath for sources in mapping
    .classPathResolver("some/path")
    // specify casing of hex numbers in IRI percent encoding (default = true)
    // added for backwards compatibility with IRI encoding up until v0.2.3
    .iriUpperCasePercentEncoding(false)
    // Specify a custom value factory supplier
    .valueFactorySupplier(ValidatingValueFactory::new)
    //---------------

    .build();

Model result = mapper.mapToModel();
```

Reactive Streams
----------------
CARML leverages [Project Reactor's](https://projectreactor.io/) implementation of
[reactive streams](https://www.reactive-streams.org/) to achieve streaming and (potentially) non-blocking processing of
mappings.

CARML exposes Reactor's [Flux](https://projectreactor.io/docs/core/release/reference/#flux) data structure. When you
execute a RML mapping using one of the `Flux` returning methods you get a `Flux<Statement>` as result.
This allows for further processing with
[the many reactive operators](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html)
available for Flux.

```java
Flux<Statement> statements = mapper.map();
// do some operations
Model result = statements.collect(ModelCollector.toModel())
    .block();
```

Support for Apache Jena
-------------------------
As CARML is built on [RDF4J](https://rdf4j.org/), the default output is either a
[`Flux`](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) of RDF4J 
[`Statement`](https://rdf4j.org/javadoc/latest/org/eclipse/rdf4j/model/Statement.html)s, or an RDF4J 
[`Model`](https://rdf4j.org/javadoc/latest/org/eclipse/rdf4j/model/Model.html).

However, CARML provides utilities to transform this output to equivalent Jena datastructures.

To use these utilities one needs to import the `io.carml.carml-converters-jena` dependency.

```xml
<dependency>
    <groupId>io.carml</groupId>
    <artifactId>carml-converters-jena</artifactId>
    <version>${carml.version}</version>
</dependency>
```

The following example shows the streaming transformation of RDF Statements to Jena Quads and subsequent collection into
a Jena Dataset using the provided utilities.

```java
Flux<Statement> statements = mapper.map();

Dataset jenaDataset = statements.map(JenaConverters::toQuad)
    .collect(JenaCollectors.toDataset());
```

Input stream extension
---------------------
When it comes to non-database sources, the current RML spec only supports the specification of file based sources in
an `rml:LogicalSource`. However, it is often very useful to be able to transform stream sources.

To this end CARML introduces the notion of 'Named Streams'. Which follows the ontology
defined [here](https://github.com/carml/carml/tree/master/carml.ttl).

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

Then the input stream can be mapped by providing a map of named input streams.

```java
RdfRmlMapper mapper = RdfRmlMapper.builder()
    .triplesMaps(mapping)    
    .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
    .build();

mapper.map(Map.of("stream-A", inputStream));
```

Note that it is possible to map several input streams. When combining named input streams with an unnamed input stream, 
the constant `RmlMapper.DEFAULT_STREAM_NAME` can be used as the name for the unnamed input stream. 

```java
RdfRmlMapper mapper = RdfRmlMapper.builder()
    .triplesMaps(mapping)
    .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
    .build();

mapper.map(Map.of("stream-A", inputStreamA, RmlMapper.DEFAULT_STREAM_NAME, defaultInputStream));
```

Function extension
------------------
A recent development related to RML is the possibility of adding functions to the mix. This is a powerful extension that
increases the expressivity of mappings immensely. Do note that the function ontology is still under development at
UGhent.

Because we believe that this extension can already be of great value, we've already adopted it in CARML.

<!--- TODO: explain that the function execution is a finisher, that is it runs the normal mapping, which creates the 
function execution triples, and the described execution is in turn evaluated and results in the term map value. --->
The way it works is, you describe the execution of a function in terms of the [Function Ontology (FnO)](https://fno.io/)
.

Take for example the SumFunction example of the [FnO spec](http://users.ugent.be/~bjdmeest/function/#complete-example).
This defines an instance `ex:sumFunction` of class `fno:Function` that is able to compute the sum of two values provided
as parameters of the function at execution time.

It also describes an instance `ex:sumExecution` of `fno:execution`, which `fno:executes` `ex:sumFunction` which descibes
an instance of an execution of the defined sum function. In this case with parameters 2 and 4.

To be able to use this in RML mappings we use executions of instances of `fno:Function` to determine the value of a term
map. The execution of a function can be seen as a post-processing step in the evaluation of a term map.

```
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix fnml: <http://semweb.mmlab.be/ns/fnml#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix fno: <https://w3id.org/function/ontology#> .
@prefix ex: <http://example.org/> .

ex:sumValuePredicateObjectMap
  rr:predicate ex:total ;
  rr:objectMap [
    a fnml:FunctionMap ;
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

A function can be created in any `.java` class. The function should be annotated with `@FnoFunction`, providing the
string value of the function IRI, and the function parameters with `@FnoParam`, providing the string value of the
function parameter IRIs.

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

The class or classes containing the annotated functions can then be registered on the mapper via
the `RmlMapper#addFunctions` method.

```java
RdfRmlMapper mapper = RdfRmlMapper.builder()
    .triplesMaps(mapping)
    .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
    .addFunctions(new YourRmlFunctions())
    .build();

Model result=mapper.mapToModel();
```

It is recommended to describe and publish new functions in terms of FnO for interpretability of mappings, and, possibly,
reuse of functions, but it's not mandatory for use in CARML.

Note that it is currently possible to specify and use function executions as parameters of other function executions in
CARML, although this is not (yet?) expressible in FnO.

XML namespace extension
-----------------------

When working with XML documents, it is often necessary specify namespaces to identify a node's qualified name. Most
XPath implementations allow you to register these namespaces, in order to be able to use them in executing XPath
expressions. In order to convey these expressions to the CARML engine, CARML introduces the class `carml:XmlDocument`
that can be used as a value of `rml:source`. An instance of  `carml:XmlDocument` can, if it is a file source, specify a
location via the `carml:url` property, and specify namespace declarations via the `carml:declaresNamespace` property.

For example, given the following XML document:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ex:bookstore xmlns:ex="http://www.example.com/books/1.0/">
    <ex:book category="children">
        <ex:title lang="en">Harry Potter</ex:title>
        <ex:author>J K. Rowling</ex:author>
        <ex:year>2005</ex:year>
        <ex:price>29.99</ex:price>
    </ex:book>
</ex:bookstore>
```

one can now use the following mapping, declaring namespaces, to use them in XPath expressions:

```
@prefix rr: <http://www.w3.org/ns/r2rml#>.
@prefix rml: <http://semweb.mmlab.be/ns/rml#>.
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix carml: <http://carml.taxonic.com/carml/> .
@prefix ex: <http://www.example.com/> .

<#SubjectMapping> a rr:TriplesMap ;
  rml:logicalSource [
    rml:source [
      a carml:Stream ;
      # or in case of a file source use:
      # carml:url "path-to-source" ;
      carml:declaresNamespace [
        carml:namespacePrefix "ex" ;
        carml:namespaceName "http://www.example.com/books/1.0/" ;
      ] ;
    ] ;
    rml:referenceFormulation ql:XPath ;
    rml:iterator "/ex:bookstore/*" ;
  ] ;

  rr:subjectMap [
    rr:template "http://www.example.com/{./ex:title}" ;
    rr:class ex:Book ;
    rr:termType rr:IRI ;
  ] ;
.

```

which yields:

```
<http://www.example.com/Harry%20Potter> a <http://www.example.com/Book> .
```

CARML in RML Test Cases
-----------------------
See the [RML implementation Report](https://rml.io/implementation-report/) for how CARML does in
the [RML test cases](https://rml.io/test-cases/).

> Note: currently we've raised [issues](https://github.com/RMLio/rml-test-cases/issues?q=is%3Aissue+author%3Apmaria+) 
> for some of the test cases which we believe are incorrect, or have an adverse effect on mapping data.


About CARML
-----------
CARML was first developed by [Taxonic](http://www.taxonic.com) in cooperation with [Kadaster](https://www.kadaster.com/)
. And is now being maintained and developed further by [Skemu](https://skemu.com).
