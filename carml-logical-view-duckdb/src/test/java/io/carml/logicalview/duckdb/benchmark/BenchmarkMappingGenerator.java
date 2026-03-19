package io.carml.logicalview.duckdb.benchmark;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Generates RML mapping documents in Turtle format for benchmark scenarios. Each mapping uses an
 * explicit {@code rml:LogicalView} with named fields, following the RML-LV specification. The view
 * fields are matched by both evaluators (DuckDB compiles to SQL, reactive uses expression
 * evaluation).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class BenchmarkMappingGenerator {

    /**
     * Generates a Turtle mapping for a JSON source with 6 predicate-object maps using template-based
     * IRI subjects and mixed literal/IRI objects.
     *
     * @param jsonFilePath absolute path to the JSON data file
     * @return the mapping as a Turtle string
     */
    static String jsonMapping(String jsonFilePath) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix rr: <http://www.w3.org/ns/r2rml#> .
                @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:PersonSource a rml:LogicalSource ;
                    rml:source [ a rml:FilePath ; rml:path "%s" ] ;
                    rml:referenceFormulation ql:JSONPath ;
                    rml:iterator "$.people[*]" .

                ex:PersonView a rml:LogicalView ;
                    rml:viewOn ex:PersonSource ;
                    rml:field [ rml:fieldName "id" ; rml:reference "$.id" ] ;
                    rml:field [ rml:fieldName "name" ; rml:reference "$.name" ] ;
                    rml:field [ rml:fieldName "email" ; rml:reference "$.email" ] ;
                    rml:field [ rml:fieldName "city" ; rml:reference "$.city" ] ;
                    rml:field [ rml:fieldName "country" ; rml:reference "$.country" ] ;
                    rml:field [ rml:fieldName "age" ; rml:reference "$.age" ] .

                ex:PersonMapping a rml:TriplesMap ;
                    rml:logicalSource ex:PersonView ;
                    rml:subjectMap [ rml:template "http://example.org/person/{id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:name ] ;
                        rml:objectMap [ rml:reference "name" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:email ] ;
                        rml:objectMap [ rml:template "mailto:{email}" ; rml:termType rml:IRI ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressLocality ] ;
                        rml:objectMap [ rml:reference "city" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressCountry ] ;
                        rml:objectMap [ rml:reference "country" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:age ] ;
                        rml:objectMap [ rml:reference "age" ; rml:datatype xsd:integer ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:personId ] ;
                        rml:objectMap [ rml:reference "id" ; rml:datatype xsd:integer ]
                    ] .
                """.formatted(jsonFilePath);
    }

    /**
     * Generates a Turtle mapping for a CSV source with 6 predicate-object maps.
     *
     * @param csvFilePath absolute path to the CSV data file
     * @return the mapping as a Turtle string
     */
    static String csvMapping(String csvFilePath) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix rr: <http://www.w3.org/ns/r2rml#> .
                @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:PersonSource a rml:LogicalSource ;
                    rml:source [ a rml:FilePath ; rml:path "%s" ] ;
                    rml:referenceFormulation ql:CSV .

                ex:PersonView a rml:LogicalView ;
                    rml:viewOn ex:PersonSource ;
                    rml:field [ rml:fieldName "id" ; rml:reference "id" ] ;
                    rml:field [ rml:fieldName "name" ; rml:reference "name" ] ;
                    rml:field [ rml:fieldName "email" ; rml:reference "email" ] ;
                    rml:field [ rml:fieldName "city" ; rml:reference "city" ] ;
                    rml:field [ rml:fieldName "country" ; rml:reference "country" ] ;
                    rml:field [ rml:fieldName "age" ; rml:reference "age" ] .

                ex:PersonMapping a rml:TriplesMap ;
                    rml:logicalSource ex:PersonView ;
                    rml:subjectMap [ rml:template "http://example.org/person/{id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:name ] ;
                        rml:objectMap [ rml:reference "name" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:email ] ;
                        rml:objectMap [ rml:template "mailto:{email}" ; rml:termType rml:IRI ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressLocality ] ;
                        rml:objectMap [ rml:reference "city" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressCountry ] ;
                        rml:objectMap [ rml:reference "country" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:age ] ;
                        rml:objectMap [ rml:reference "age" ; rml:datatype xsd:integer ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:personId ] ;
                        rml:objectMap [ rml:reference "id" ; rml:datatype xsd:integer ]
                    ] .
                """.formatted(csvFilePath);
    }

    /**
     * Generates a Turtle mapping with two TriplesMap instances reading the same JSON source. Both
     * maps share the same LogicalSource and LogicalView fields, but produce different triple
     * patterns. This exercises the single-read source cache: the resolver should parse the JSON file
     * once, caching records for both views.
     *
     * @param jsonFilePath absolute path to the JSON data file
     * @return the mapping as a Turtle string
     */
    static String sharedSourceJsonMapping(String jsonFilePath) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix rr: <http://www.w3.org/ns/r2rml#> .
                @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:PersonSource a rml:LogicalSource ;
                    rml:source [ a rml:FilePath ; rml:path "%1$s" ] ;
                    rml:referenceFormulation ql:JSONPath ;
                    rml:iterator "$.people[*]" .

                ex:PersonView a rml:LogicalView ;
                    rml:viewOn ex:PersonSource ;
                    rml:field [ rml:fieldName "id" ; rml:reference "$.id" ] ;
                    rml:field [ rml:fieldName "name" ; rml:reference "$.name" ] ;
                    rml:field [ rml:fieldName "email" ; rml:reference "$.email" ] ;
                    rml:field [ rml:fieldName "city" ; rml:reference "$.city" ] ;
                    rml:field [ rml:fieldName "country" ; rml:reference "$.country" ] ;
                    rml:field [ rml:fieldName "age" ; rml:reference "$.age" ] .

                ex:PersonMapping a rml:TriplesMap ;
                    rml:logicalSource ex:PersonView ;
                    rml:subjectMap [ rml:template "http://example.org/person/{id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:name ] ;
                        rml:objectMap [ rml:reference "name" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:email ] ;
                        rml:objectMap [ rml:template "mailto:{email}" ; rml:termType rml:IRI ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressLocality ] ;
                        rml:objectMap [ rml:reference "city" ]
                    ] .

                ex:ContactMapping a rml:TriplesMap ;
                    rml:logicalSource ex:PersonView ;
                    rml:subjectMap [ rml:template "http://example.org/contact/{id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:addressCountry ] ;
                        rml:objectMap [ rml:reference "country" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:age ] ;
                        rml:objectMap [ rml:reference "age" ; rml:datatype xsd:integer ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:personId ] ;
                        rml:objectMap [ rml:reference "id" ; rml:datatype xsd:integer ]
                    ] .
                """.formatted(jsonFilePath);
    }

    /**
     * Generates a Turtle mapping for a JSON source with iterable fields (nested arrays). Produces
     * triples for each item-tag combination, exercising UNNEST / iterable field evaluation.
     *
     * @param jsonFilePath absolute path to the JSON data file with nested arrays
     * @return the mapping as a Turtle string
     */
    static String nestedJsonMapping(String jsonFilePath) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix rr: <http://www.w3.org/ns/r2rml#> .
                @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                ex:ItemSource a rml:LogicalSource ;
                    rml:source [ a rml:FilePath ; rml:path "%s" ] ;
                    rml:referenceFormulation ql:JSONPath ;
                    rml:iterator "$.items[*]" .

                ex:ItemView a rml:LogicalView ;
                    rml:viewOn ex:ItemSource ;
                    rml:field [ rml:fieldName "id" ; rml:reference "$.id" ] ;
                    rml:field [ rml:fieldName "name" ; rml:reference "$.name" ] ;
                    rml:field [
                        a rml:IterableField ;
                        rml:fieldName "tag" ;
                        rml:iterator "$.tags[*]" ;
                        rml:field [ rml:fieldName "value" ; rml:reference "$" ]
                    ] .

                ex:ItemMapping a rml:TriplesMap ;
                    rml:logicalSource ex:ItemView ;
                    rml:subjectMap [ rml:template "http://example.org/item/{id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:name ] ;
                        rml:objectMap [ rml:reference "name" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant ex:tag ] ;
                        rml:objectMap [ rml:reference "tag.value" ]
                    ] .
                """.formatted(jsonFilePath);
    }
}
