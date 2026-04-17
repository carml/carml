package io.carml.testcases.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.logicalview.sql.ReactiveSqlLogicalViewEvaluatorFactory;
import io.carml.logicalview.sql.mysql.MySqlClientProvider;
import io.carml.logicalview.sql.postgresql.PostgreSqlClientProvider;
import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import io.vertx.core.Vertx;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Benchmark comparing the reactive SQL evaluator (Vert.x) against the DuckDB evaluator for SQL
 * database sources. Measures end-to-end wall time and throughput for simple table mappings and
 * join mappings at various data scales.
 *
 * <p>Run with:
 * <pre>{@code
 * mvn test -pl carml-test-cases -Dcheckstyle.skip=true -Dspotless.check.skip=true \
 *     -Dtest="SqlEvaluatorBenchmarkTest"
 * }</pre>
 */
@Slf4j
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
class SqlEvaluatorBenchmarkTest {

    private static final int WARM_UP_ROWS = 100;
    private static final int[] ROW_COUNTS = {100, 1_000, 10_000, 100_000};

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bench")
            .withUsername("root")
            .withPassword("bench");

    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("bench")
            .withUsername("postgres")
            .withPassword("bench");

    private Vertx vertx;
    private ReactiveSqlLogicalViewEvaluatorFactory mysqlReactiveFactory;
    private ReactiveSqlLogicalViewEvaluatorFactory pgReactiveFactory;

    @BeforeAll
    void setUp() {
        vertx = Vertx.vertx();
        mysqlReactiveFactory = new ReactiveSqlLogicalViewEvaluatorFactory(List.of(new MySqlClientProvider()), vertx);
        pgReactiveFactory = new ReactiveSqlLogicalViewEvaluatorFactory(List.of(new PostgreSqlClientProvider()), vertx);
    }

    @AfterAll
    void tearDown() {
        if (mysqlReactiveFactory != null) {
            mysqlReactiveFactory.close();
        }
        if (pgReactiveFactory != null) {
            pgReactiveFactory.close();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    // --- MySQL benchmarks ---

    @Test
    void benchmark_mysql_simpleTable() throws SQLException {
        LOG.info("=== MySQL Simple Table Benchmark ===");
        LOG.info("{}", formatHeader());

        loadMySqlData(WARM_UP_ROWS);
        var warmUpMapping = mysqlMapping("person", MYSQL.getJdbcUrl(), "bench");
        runDuckDbMySQL(warmUpMapping);
        runReactiveMySQL(warmUpMapping);

        for (int count : ROW_COUNTS) {
            loadMySqlData(count);
            var mapping = mysqlMapping("person", MYSQL.getJdbcUrl(), "bench");

            var duckResult = runDuckDbMySQL(mapping);
            var reactResult = runReactiveMySQL(mapping);

            LOG.info("{}", formatRow("MySQL", count, "DuckDB", duckResult));
            LOG.info("{}", formatRow("MySQL", count, "Reactive", reactResult));
            LOG.info("{}", formatSpeedup("MySQL", count, duckResult.durationMs, reactResult.durationMs));
            LOG.info("");

            assertThat(duckResult.tripleCount, greaterThan(0L));
            assertThat(reactResult.tripleCount, greaterThan(0L));
            assertThat(
                    "Both evaluators should produce same triple count",
                    reactResult.tripleCount,
                    is(duckResult.tripleCount));
        }
    }

    @Test
    void benchmark_mysql_joinTwoTables() throws SQLException {
        LOG.info("=== MySQL Join (2 tables) Benchmark ===");
        LOG.info("{}", formatHeader());

        loadMySqlJoinData(WARM_UP_ROWS);
        var warmUpMapping = mysqlJoinMapping(MYSQL.getJdbcUrl(), "bench");
        runDuckDbMySQL(warmUpMapping);
        runReactiveMySQL(warmUpMapping);

        for (int count : ROW_COUNTS) {
            loadMySqlJoinData(count);
            var mapping = mysqlJoinMapping(MYSQL.getJdbcUrl(), "bench");

            var duckResult = runDuckDbMySQL(mapping);
            var reactResult = runReactiveMySQL(mapping);

            LOG.info("{}", formatRow("MySQL JOIN", count, "DuckDB", duckResult));
            LOG.info("{}", formatRow("MySQL JOIN", count, "Reactive", reactResult));
            LOG.info("{}", formatSpeedup("MySQL JOIN", count, duckResult.durationMs, reactResult.durationMs));
            LOG.info("");

            assertThat(duckResult.tripleCount, greaterThan(0L));
            assertThat(reactResult.tripleCount, greaterThan(0L));
        }
    }

    // --- PostgreSQL benchmarks ---

    @Test
    void benchmark_postgresql_simpleTable() throws SQLException {
        LOG.info("=== PostgreSQL Simple Table Benchmark ===");
        LOG.info("{}", formatHeader());

        loadPostgreSqlData(WARM_UP_ROWS);
        var warmUpMapping = postgresqlMapping("person", POSTGRESQL.getJdbcUrl(), "bench");
        runDuckDbPostgreSQL(warmUpMapping);
        runReactivePostgreSQL(warmUpMapping);

        for (int count : ROW_COUNTS) {
            loadPostgreSqlData(count);
            var mapping = postgresqlMapping("person", POSTGRESQL.getJdbcUrl(), "bench");

            var duckResult = runDuckDbPostgreSQL(mapping);
            var reactResult = runReactivePostgreSQL(mapping);

            LOG.info("{}", formatRow("PostgreSQL", count, "DuckDB", duckResult));
            LOG.info("{}", formatRow("PostgreSQL", count, "Reactive", reactResult));
            LOG.info("{}", formatSpeedup("PostgreSQL", count, duckResult.durationMs, reactResult.durationMs));
            LOG.info("");

            assertThat(duckResult.tripleCount, greaterThan(0L));
            assertThat(reactResult.tripleCount, greaterThan(0L));
            assertThat(
                    "Both evaluators should produce same triple count",
                    reactResult.tripleCount,
                    is(duckResult.tripleCount));
        }
    }

    // --- Cursor batch size sweep ---

    @Test
    void benchmark_mysql_cursorBatchSizeSweep() throws SQLException {
        LOG.info("=== MySQL Cursor Batch Size Sweep (10K rows) ===");
        LOG.info(
                "{}",
                "%-12s | %8s | %8s | %12s | %8s"
                        .formatted("BatchSize", "Time(ms)", "Triples", "Triples/sec", "Mem(MB)"));

        int rowCount = 10_000;
        loadMySqlData(rowCount);
        var mapping = mysqlMapping("person", MYSQL.getJdbcUrl(), "bench");

        int[] batchSizes = {32, 64, 128, 256, 512, 1024, 2048, 4096};

        // Warm up with default batch size
        runMapping(mapping, mysqlReactiveFactory);

        for (int batchSize : batchSizes) {
            // Don't close factories — they share the Vertx instance from setUp()
            var factory =
                    new ReactiveSqlLogicalViewEvaluatorFactory(List.of(new MySqlClientProvider()), vertx, batchSize);
            var result = runMapping(mapping, factory);

            LOG.info(
                    "{}",
                    "%-12d | %8d | %8d | %12.0f | %8s"
                            .formatted(
                                    batchSize,
                                    result.durationMs,
                                    result.tripleCount,
                                    result.triplesPerSecond(),
                                    result.memMb()));
        }
    }

    // --- Pool size sweep ---

    @Test
    void benchmark_mysql_poolSizeSweep() throws SQLException {
        LOG.info("=== MySQL Pool Size Sweep (10K rows) ===");
        LOG.info(
                "{}",
                "%-12s | %8s | %8s | %12s | %8s"
                        .formatted("PoolSize", "Time(ms)", "Triples", "Triples/sec", "Mem(MB)"));

        int rowCount = 10_000;
        loadMySqlData(rowCount);
        var mapping = mysqlMapping("person", MYSQL.getJdbcUrl(), "bench");

        int[] poolSizes = {1, 2, 4, 8, 16, 32};

        // Warm up
        runMapping(mapping, mysqlReactiveFactory);

        for (int poolSize : poolSizes) {
            var factory = new ReactiveSqlLogicalViewEvaluatorFactory(
                    List.of(new MySqlClientProvider()), vertx, 2048, poolSize);
            var result = runMapping(mapping, factory);

            LOG.info(
                    "{}",
                    "%-12d | %8d | %8d | %12.0f | %8s"
                            .formatted(
                                    poolSize,
                                    result.durationMs,
                                    result.tripleCount,
                                    result.triplesPerSecond(),
                                    result.memMb()));
        }
    }

    // --- Data loading ---

    private void loadMySqlData(int rowCount) throws SQLException {
        try (var conn = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "bench");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS person");
            stmt.execute("""
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        email VARCHAR(200),
                        city VARCHAR(100),
                        country VARCHAR(100),
                        age INTEGER
                    )""");

            insertBatched(
                    stmt,
                    "INSERT INTO person VALUES ",
                    rowCount,
                    i -> "(%d,'Person_%d','person_%d@example.com','City_%d','Country_%d',%d)"
                            .formatted(i, i, i, i % 50, i % 20, 20 + (i % 60)));
        }
    }

    private void loadMySqlJoinData(int rowCount) throws SQLException {
        try (var conn = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "bench");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS employee");
            stmt.execute("DROP TABLE IF EXISTS department");
            stmt.execute("""
                    CREATE TABLE department (
                        dept_id INTEGER PRIMARY KEY,
                        dept_name VARCHAR(100)
                    )""");
            stmt.execute("""
                    CREATE TABLE employee (
                        emp_id INTEGER PRIMARY KEY,
                        emp_name VARCHAR(100),
                        dept_id INTEGER
                    )""");

            // 10 departments
            var deptSb = new StringBuilder("INSERT INTO department VALUES ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    deptSb.append(',');
                }
                deptSb.append("(%d,'Department_%d')".formatted(i, i));
            }
            stmt.execute(deptSb.toString());

            insertBatched(
                    stmt,
                    "INSERT INTO employee VALUES ",
                    rowCount,
                    i -> "(%d,'Employee_%d',%d)".formatted(i, i, i % 10));
        }
    }

    private void loadPostgreSqlData(int rowCount) throws SQLException {
        try (var conn = DriverManager.getConnection(POSTGRESQL.getJdbcUrl(), "postgres", "bench");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS person");
            stmt.execute("""
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        email VARCHAR(200),
                        city VARCHAR(100),
                        country VARCHAR(100),
                        age INTEGER
                    )""");

            insertBatched(
                    stmt,
                    "INSERT INTO person VALUES ",
                    rowCount,
                    i -> "(%d,'Person_%d','person_%d@example.com','City_%d','Country_%d',%d)"
                            .formatted(i, i, i, i % 50, i % 20, 20 + (i % 60)));
        }
    }

    /**
     * Inserts rows in batches of 1000 to avoid exceeding MySQL's max_allowed_packet.
     */
    private static void insertBatched(
            Statement stmt, String insertPrefix, int totalRows, java.util.function.IntFunction<String> rowGenerator)
            throws SQLException {
        int batchSize = 1000;
        for (int start = 0; start < totalRows; start += batchSize) {
            int end = Math.min(start + batchSize, totalRows);
            var sb = new StringBuilder(insertPrefix);
            for (int i = start; i < end; i++) {
                if (i > start) {
                    sb.append(',');
                }
                sb.append(rowGenerator.apply(i));
            }
            stmt.execute(sb.toString());
        }
    }

    // --- Mapping execution ---

    private BenchmarkResult runDuckDbMySQL(String turtleMapping) {
        return runWithDuckDb(turtleMapping);
    }

    private BenchmarkResult runDuckDbPostgreSQL(String turtleMapping) {
        return runWithDuckDb(turtleMapping);
    }

    /**
     * Runs a mapping through the DuckDB evaluator. The {@link DuckDbDatabaseAttacher} inside the
     * factory automatically INSTALLs, LOADs, and ATTACHes the database when it encounters the JDBC
     * DSN in the mapping's {@code DatabaseSource}. No manual ATTACH or USE is needed.
     */
    private BenchmarkResult runWithDuckDb(String turtleMapping) {
        try (var conn = DriverManager.getConnection("jdbc:duckdb:")) {
            var factory = new DuckDbLogicalViewEvaluatorFactory(conn);
            return runMapping(turtleMapping, factory);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to run DuckDB benchmark", e);
        }
    }

    private BenchmarkResult runReactiveMySQL(String turtleMapping) {
        return runMapping(turtleMapping, mysqlReactiveFactory);
    }

    private BenchmarkResult runReactivePostgreSQL(String turtleMapping) {
        return runMapping(turtleMapping, pgReactiveFactory);
    }

    private BenchmarkResult runMapping(String turtleMapping, Object evaluatorFactory) {
        var mappingStream = new ByteArrayInputStream(turtleMapping.getBytes(StandardCharsets.UTF_8));
        Set<TriplesMap> triplesMaps = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .triplesMaps(triplesMaps);

        if (evaluatorFactory instanceof DuckDbLogicalViewEvaluatorFactory duckFactory) {
            mapperBuilder.logicalViewEvaluatorFactory(duckFactory);
        } else if (evaluatorFactory instanceof ReactiveSqlLogicalViewEvaluatorFactory reactFactory) {
            mapperBuilder.logicalViewEvaluatorFactory(reactFactory);
        }

        var mapper = mapperBuilder.build();

        // Stream and count — no Model collection, so heap stays bounded to what the reactive
        // pipeline needs per batch, not the entire result set.
        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        long tripleCount =
                java.util.Objects.requireNonNullElse(mapper.map().count().block(), 0L);
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long peakMemBytes = Math.max(0, memAfter - memBefore);

        return new BenchmarkResult(tripleCount, durationMs, peakMemBytes);
    }

    // --- Mapping generators ---

    private static String mysqlMapping(String tableName, String jdbcUrl, String password) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:DB a d2rq:Database ;
                    d2rq:jdbcDSN "%s" ;
                    d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver" ;
                    d2rq:username "root" ;
                    d2rq:password "%s" .

                ex:PersonSource a rml:LogicalSource ;
                    rml:source ex:DB ;
                    rml:referenceFormulation rml:SQL2008Table ;
                    rml:iterator "%s" .

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
                """.formatted(jdbcUrl, password, tableName);
    }

    private static String mysqlJoinMapping(String jdbcUrl, String password) {
        return """
                @prefix rml: <http://w3id.org/rml/> .
                @prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                ex:DB a d2rq:Database ;
                    d2rq:jdbcDSN "%s" ;
                    d2rq:jdbcDriver "com.mysql.cj.jdbc.Driver" ;
                    d2rq:username "root" ;
                    d2rq:password "%s" .

                ex:EmpSource a rml:LogicalSource ;
                    rml:source ex:DB ;
                    rml:referenceFormulation rml:SQL2008Table ;
                    rml:iterator "employee" .

                ex:DeptSource a rml:LogicalSource ;
                    rml:source ex:DB ;
                    rml:referenceFormulation rml:SQL2008Table ;
                    rml:iterator "department" .

                ex:DeptView a rml:LogicalView ;
                    rml:viewOn ex:DeptSource ;
                    rml:field [ rml:fieldName "dept_id" ; rml:reference "dept_id" ] ;
                    rml:field [ rml:fieldName "dept_name" ; rml:reference "dept_name" ] .

                ex:EmpView a rml:LogicalView ;
                    rml:viewOn ex:EmpSource ;
                    rml:field [ rml:fieldName "emp_id" ; rml:reference "emp_id" ] ;
                    rml:field [ rml:fieldName "emp_name" ; rml:reference "emp_name" ] ;
                    rml:field [ rml:fieldName "dept_id" ; rml:reference "dept_id" ] ;
                    rml:leftJoin [
                        rml:parentLogicalView ex:DeptView ;
                        rml:joinCondition [ rml:child "dept_id" ; rml:parent "dept_id" ] ;
                        rml:field [ rml:fieldName "department" ; rml:reference "dept_name" ]
                    ] .

                ex:EmpMapping a rml:TriplesMap ;
                    rml:logicalSource ex:EmpView ;
                    rml:subjectMap [ rml:template "http://example.org/employee/{emp_id}" ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:name ] ;
                        rml:objectMap [ rml:reference "emp_name" ]
                    ] ;
                    rml:predicateObjectMap [
                        rml:predicateMap [ rml:constant schema:department ] ;
                        rml:objectMap [ rml:reference "department" ]
                    ] .
                """.formatted(jdbcUrl, password);
    }

    private static String postgresqlMapping(String tableName, String jdbcUrl, String password) {
        return mysqlMapping(tableName, jdbcUrl, password)
                .replace("com.mysql.cj.jdbc.Driver", "org.postgresql.Driver")
                .replace("d2rq:username \"root\"", "d2rq:username \"postgres\"");
    }

    // --- Result types ---

    private record BenchmarkResult(long tripleCount, long durationMs, long memDeltaBytes) {

        double triplesPerSecond() {
            return durationMs > 0 ? (tripleCount * 1000.0) / durationMs : 0;
        }

        String memMb() {
            return "%.1f".formatted(memDeltaBytes / (1024.0 * 1024.0));
        }
    }

    // --- Formatting ---

    private static String formatHeader() {
        return "%-14s | %8s | %10s | %8s | %12s | %8s"
                .formatted("Database", "Rows", "Evaluator", "Time(ms)", "Triples/sec", "Mem(MB)");
    }

    private static String formatRow(String db, int rows, String evaluator, BenchmarkResult result) {
        return "%-14s | %8d | %10s | %8d | %12.0f | %8s"
                .formatted(db, rows, evaluator, result.durationMs, result.triplesPerSecond(), result.memMb());
    }

    private static String formatSpeedup(String db, int rows, long duckMs, long reactiveMs) {
        if (duckMs <= 0 || reactiveMs <= 0) {
            return "%-14s | %8d | %10s | %s".formatted(db, rows, "Speedup", "N/A (too fast)");
        }
        double ratio = (double) duckMs / reactiveMs;
        String winner = ratio > 1.0 ? "Reactive faster" : "DuckDB faster";
        return "%-14s | %8d | %10s | %.2fx (%s)".formatted(db, rows, "Speedup", ratio, winner);
    }
}
