package io.carml.testcases.rml.io;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;

class TestRmlIoTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/io/test-cases";
    }

    /**
     * Skip list for the rml-io conformance suite when running through the DuckDB evaluator,
     * audited fresh against the upstream test cases as of the 2026-04-20 sync. Differs from the
     * baseline {@link TestRmlIoTestCases} skip list in two ways:
     *
     * <ul>
     *   <li>DuckDB's CSV reader auto-infers column types, so a number of CSV test cases whose
     *       fixtures expect {@code "33"^^xsd:integer} pass through DuckDB even though the
     *       fixtures' expectations are spec-incorrect. Those tests are NOT in this skip list.
     *   <li>DuckDB has no XML support, so XML/XPath test cases fail with a different error
     *       (no evaluator matched the logical view) and stay skipped here even though some
     *       pass on the baseline reactive evaluator.
     * </ul>
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // ====================================================================
                // DuckDB-specific engine gaps.
                // ====================================================================

                // UTF-16 LE BOM JSON source. DuckDB's read_text helper called by the source
                // table cache fails on non-UTF-8 bytes; the source materialisation falls
                // back to a direct read which itself errors. Failure: DuckDbQueryException
                // ("Failed to execute DuckDB query for view ...").
                "RMLSTC0001b",

                // D2RQ-database source. The mapping declares
                // `rml:referenceFormulation rml:SQL2008Table` over a `d2rq:Database` source
                // but does NOT declare `d2rq:jdbcDriver` (and the DSN is the placeholder
                // `$CONNECTIONDSN`). The DuckDB attacher needs a driver to pick a scanner
                // extension; with `dbSource.getJdbcDriver()` returning null the lookup in
                // its driver→extension map blows up:
                //   NullPointerException at DuckDbDatabaseAttacher.doAttach
                // Even with a driver declared, the rml-io suite has no live database for
                // the placeholder DSN — SQL test fixtures live in the rml-io-registry suite
                // which spins up real databases.
                "RMLSTC0006a",

                // XML / XPath sources are not handled by the DuckDB evaluator (no native XML
                // scanner, no SQL-shaped translation). The evaluator factory returns no match
                // and the dispatch fails with "No evaluator matched logical view". Note: in the
                // RmlTestCaseSuite single-evaluator harness this is the only registered factory,
                // so there is no fallback to the reactive evaluator the way carml-jar gets in
                // `auto` mode.
                "RMLSTC0007c",
                "RMLSTC0007d",
                "RMLSTC0012a",
                "RMLSTC0012b",
                "RMLSTC0012c",
                "RMLSTC0012d",
                "RMLSTC0012e",

                // ====================================================================
                // Upstream test fixture bug — incomplete natural-typing fix coverage.
                //
                // Mirrors the same skip block in {@link TestRmlIoTestCases}. Both
                // evaluators produce identical {@code "33"^^xsd:integer} output for these
                // fixtures (per the rml-io-registry json-path natural-rdf-mapping spec),
                // so the divergence is on the fixture side. Upstream commit d35ce9a
                // ("fix: Add natural datatype integer in the expected output", 2026-04-29)
                // updated many TTC fixtures but missed the format / encoding / compression
                // sibling variants below — the fixed canonical {@code .nq} sibling is
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
                // Test-case bugs — individual fixture issues; same root causes as the
                // baseline IO suite. See TestRmlIoTestCases for the per-test rationale.
                // ====================================================================
                "RMLSTC0009a");
    }
}
