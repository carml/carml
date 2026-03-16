package io.carml.logicalview.duckdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Generates synthetic test data files at various scales for benchmarking. Creates JSON and CSV files
 * with a realistic multi-field schema: id, name, email, city, country, age, score, active.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class BenchmarkDataGenerator {

    /**
     * Generates a JSON file containing an array of records under the key {@code "people"}.
     *
     * <p>Each record has 8 fields: id, name, email, city, country, age, score, active. This
     * structure exercises string, integer, double, and boolean types in both evaluators.
     *
     * @param dir the directory to write the file into
     * @param recordCount the number of records to generate
     * @return the path to the generated JSON file
     * @throws IOException if file writing fails
     */
    static Path generateJson(Path dir, int recordCount) throws IOException {
        var path = dir.resolve("benchmark_%d.json".formatted(recordCount));
        var sb = new StringBuilder(recordCount * 200);
        sb.append("{\"people\":[\n");
        for (int i = 0; i < recordCount; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(jsonRecord(i));
        }
        sb.append("\n]}");
        Files.writeString(path, sb.toString());
        return path;
    }

    /**
     * Generates a CSV file with a header row followed by the specified number of data rows.
     *
     * <p>Same schema as JSON: id, name, email, city, country, age, score, active.
     *
     * @param dir the directory to write the file into
     * @param recordCount the number of data rows to generate
     * @return the path to the generated CSV file
     * @throws IOException if file writing fails
     */
    static Path generateCsv(Path dir, int recordCount) throws IOException {
        var path = dir.resolve("benchmark_%d.csv".formatted(recordCount));
        var sb = new StringBuilder(recordCount * 120);
        sb.append("id,name,email,city,country,age,score,active\n");
        for (int i = 0; i < recordCount; i++) {
            sb.append(csvRow(i));
            sb.append('\n');
        }
        Files.writeString(path, sb.toString());
        return path;
    }

    /**
     * Generates a JSON file containing records with a nested array field {@code tags}, suitable for
     * benchmarking iterable field / UNNEST performance.
     *
     * @param dir the directory to write the file into
     * @param recordCount the number of records to generate
     * @param tagsPerRecord the number of tags per record
     * @return the path to the generated JSON file
     * @throws IOException if file writing fails
     */
    static Path generateJsonWithNested(Path dir, int recordCount, int tagsPerRecord) throws IOException {
        var path = dir.resolve("benchmark_nested_%d.json".formatted(recordCount));
        var sb = new StringBuilder(recordCount * (200 + tagsPerRecord * 20));
        sb.append("{\"items\":[\n");
        for (int i = 0; i < recordCount; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(nestedJsonRecord(i, tagsPerRecord));
        }
        sb.append("\n]}");
        Files.writeString(path, sb.toString());
        return path;
    }

    private static String jsonRecord(int index) {
        var city = CITIES[index % CITIES.length];
        var country = COUNTRIES[index % COUNTRIES.length];
        return """
                {"id":%d,"name":"Person_%d","email":"person_%d@example.org","city":"%s","country":"%s","age":%d,"score":%s,"active":%s}""".formatted(
                        index,
                        index,
                        index,
                        city,
                        country,
                        20 + (index % 60),
                        "%.2f".formatted(50.0 + (index % 500) / 10.0),
                        index % 3 != 0);
    }

    private static String csvRow(int index) {
        var city = CITIES[index % CITIES.length];
        var country = COUNTRIES[index % COUNTRIES.length];
        return "%d,Person_%d,person_%d@example.org,%s,%s,%d,%.2f,%s"
                .formatted(
                        index,
                        index,
                        index,
                        city,
                        country,
                        20 + (index % 60),
                        50.0 + (index % 500) / 10.0,
                        index % 3 != 0);
    }

    private static String nestedJsonRecord(int index, int tagsPerRecord) {
        var tags = IntStream.range(0, tagsPerRecord)
                .mapToObj(t -> "\"tag_%d_%d\"".formatted(index, t))
                .collect(Collectors.joining(","));
        return """
                {"id":%d,"name":"Item_%d","tags":[%s]}""".formatted(index, index, tags);
    }

    private static final String[] CITIES = {
        "Amsterdam", "Berlin", "Copenhagen", "Dublin", "Edinburgh",
        "Florence", "Geneva", "Helsinki", "Istanbul", "Jakarta"
    };

    private static final String[] COUNTRIES = {
        "Netherlands", "Germany", "Denmark", "Ireland", "Scotland",
        "Italy", "Switzerland", "Finland", "Turkey", "Indonesia"
    };
}
