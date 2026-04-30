package io.carml.testcases.rml.io;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlIoTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/io/test-cases";
    }

    /**
     * Skip list for the rml-io conformance suite, audited fresh against the upstream test cases as
     * of the 2026-04-20 sync. Every entry has been verified to still fail in the current state and
     * the rationale below reflects what CARML actually emits vs. what the test fixture expects.
     *
     * <p>Tests are grouped by the underlying root cause. Within each group, each ID is a separate
     * conformance fixture that exhibits the same failure mode for the same reason.
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // ====================================================================
                // Engine gaps — the test exercises a feature CARML does not implement.
                // ====================================================================

                // D2RQ-database source. The mapping declares
                // `rml:referenceFormulation rml:SQL2008Table` over a `d2rq:Database` source with
                // a placeholder DSN (`$CONNECTIONDSN`/`$USERNAME`/`$PASSWORD`). CARML's
                // {@code CarmlLogicalSource.getReferenceFormulation()} contains a legacy
                // R2RML compatibility shim that overrides the declared formulation to
                // {@code ql:Rdb} whenever the source is a {@code DatabaseSource}. The plain
                // rml-io suite registers only the default (non-SQL) evaluator — no Vert.x
                // SQL client and no Testcontainers database — so no resolver matches
                // {@code ql:Rdb} and the run fails with:
                //   LogicalSourceResolverException: No logical source resolver found for
                //   reference formulation ...ql#Rdb. Resolvers available: CsvResolver,
                //   JsonPathResolver, XPathResolver
                // SQL test fixtures live in the rml-io-registry suite which spins up real
                // databases; this rml-io fixture cannot pass without one.
                "RMLSTC0006a",

                // ====================================================================
                // Upstream test fixture bug: 0004a's mapping declares no rml:null, but its
                // expected default.nq excludes id=5 (the empty-cells row) — same as 0004b/c
                // which DO declare rml:null "". Per the RML-IO CSV reference-formulation spec
                // (https://w3id.org/rml/io/spec/#csv-reference-formulation), empty CSV cells
                // are values, not null, by default. Either 0004a's mapping needs rml:null ""
                // added, or its expected output needs the empty-string triples.
                // ====================================================================
                // "RMLSTC0004a",  // re-check after upstream fix

                // ====================================================================
                // Upstream test fixture bug — incomplete natural-typing fix coverage.
                //
                // Per `rml-io-registry/json-path/section/natural-rdf-mapping.md`, JSON
                // `number` without fraction maps to `xsd:integer`. CARML emits the
                // spec-correct `"33"^^xsd:integer` for the JSON `"age": 33` field in
                // `Friends.json`. Upstream commit d35ce9a ("fix: Add natural datatype
                // integer in the expected output", 2026-04-29) updated many TTC fixtures
                // but missed the sibling variants below. These are the same mapping in
                // alternative output formats / encodings / compressions — fixed nq sibling
                // listed for each.
                //
                // Pattern: missing plain `"33"`, unexpected `"33"^^xsd:integer`.
                // ====================================================================
                "RMLTTC0004a", // .jsonld variant of 0004d.dump1.nq (fixed)
                "RMLTTC0004c", // .nt variant of 0004d
                "RMLTTC0004f", // .rdfxml variant of 0004d
                "RMLTTC0004g", // .ttl variant of 0004d
                "RMLTTC0005b", // UTF-16 variant of 0005a (fixed)
                "RMLTTC0006b", // gzip variant of 0006a (fixed)
                "RMLTTC0006c", // zip variant of 0006a
                "RMLTTC0006d", // tar.xz variant of 0006a
                "RMLTTC0006e", // tar.gz variant of 0006a

                // RMLTTC0004e: RDF/JSON variant of 0004d. Upstream commit 35eb678 added
                // the required `"type": "literal"` so RDF4J's RDF/JSON parser no longer
                // rejects the file, but two issues remain: each value is still a JSON
                // number (`"value": 33`) instead of a string (`"value": "33"`), and no
                // `"datatype"` field is present. Once parsed the literals are plain
                // strings, so the same `xsd:integer` mismatch applies.
                "RMLTTC0004e",

                // ====================================================================
                // Test-case bugs — individual fixture issues, each with a distinct cause.
                // ====================================================================

                // RMLSTC0009a: fixture declares `hasError=true` for a CSV with quoted header
                // fields (`"id","name","age"`) and a mapping that references the columns
                // unquoted (`id`, `name`, `age`). RFC 4180 §2.5 — which the rml-io-registry
                // CSV spec cites as `[[CSV]]` — leaves "quoted vs unquoted field value
                // equivalence" implied but not explicit (its ABNF has
                // `field = (escaped / non-escaped)` with the DQUOTEs wrapping the content of
                // an `escaped` field). The W3C CSVW tabular-data-model spec §4.5 makes it
                // explicit: "presence or absence of quotes around a value within a CSV file is
                // a syntactic detail that is not reflected in the tabular data model." Under
                // both interpretations the parsed column names are `id`, `name`, `age`, so the
                // unquoted mapping references match. CARML's CSV reader (FastCSV) does this,
                // triples are produced correctly, and the `hasError=true` expectation
                // contradicts the conventional CSV interpretation. Failure:
                // AssertionFailedError ("Expected RuntimeException to be thrown, but nothing
                // was thrown").
                "RMLSTC0009a");
    }
}
