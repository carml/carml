package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.service.AutoService;
import com.google.common.collect.Iterables;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvReader.CsvReaderBuilder;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import de.siegmar.fastcsv.reader.NamedCsvRecordHandler;
import io.carml.csv.CsvDialectConfig;
import io.carml.csv.CsvNullValueHandler;
import io.carml.csv.CsvProcessingException;
import io.carml.csv.CsvwDialectProcessor;
import io.carml.logicalsourceresolver.sourceresolver.Encodings;
import io.carml.model.CsvReferenceFormulation;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.util.TypeRef;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.BOMInputStream;
import org.eclipse.rdf4j.model.IRI;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class CsvResolver implements LogicalSourceResolver<NamedCsvRecord> {

    public static final String NAME = "CsvResolver";

    public static LogicalSourceResolverFactory<NamedCsvRecord> factory() {
        return factory(Options.builder().build());
    }

    public static LogicalSourceResolverFactory<NamedCsvRecord> factory(Options options) {
        return source -> new CsvResolver(source, options);
    }

    private final Source source;

    private final Options options;

    private CsvResolver(Source source, Options options) {
        this.source = source;
        this.options = options;
    }

    /**
     * Configuration options for {@link CsvResolver}.
     *
     * <p>Use {@link Options#builder()} to construct an instance. All fields have sensible defaults
     * that enforce strict, RML-spec-compliant CSV parsing.
     */
    @Builder
    @Getter
    @ToString
    public static class Options {

        /**
         * Factory for creating a fresh {@link CsvReaderBuilder} per source resolution. A new builder
         * is requested each time because CSVW dialect settings (delimiter, quote character, etc.)
         * mutate the builder, so each resolution needs its own instance.
         *
         * <p>Override this to customize FastCSV parsing behavior, for example to allow extra
         * characters after closing quotes via
         * {@link CsvReaderBuilder#allowExtraCharsAfterClosingQuote(boolean)}.
         *
         * <p>Defaults to {@link CsvReader#builder}.
         */
        @Builder.Default
        private final Supplier<CsvReaderBuilder> csvReaderBuilderFactory = CsvReader::builder;

        /**
         * Whether to validate that each data record has the same number of fields as the header.
         * When enabled, a {@link LogicalSourceResolverException} is thrown for any record whose
         * field count differs from the header count.
         *
         * <p>Note: FastCSV 4.x only checks data-vs-data field count consistency internally; it does
         * not validate data records against the header for {@link NamedCsvRecord}. This option fills
         * that gap.
         *
         * <p>Defaults to {@code true} (strict, RML-spec-compliant).
         */
        @Builder.Default
        private final boolean strictFieldCount = true;

        /**
         * Whether to allow duplicate column names in the CSV header row. When {@code false},
         * duplicate header names cause a parse error.
         *
         * <p>Defaults to {@code false} (duplicates rejected).
         */
        @Builder.Default
        private final boolean allowDuplicateHeaders = false;
    }

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<NamedCsvRecord>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSourceFilter) {
        return resolvedSource -> getCsvRecordFlux(resolvedSource, logicalSourceFilter);
    }

    @SuppressWarnings("unchecked")
    private Flux<LogicalSourceRecord<NamedCsvRecord>> getCsvRecordFlux(
            ResolvedSource<?> resolvedSource, Set<LogicalSource> logicalSources) {
        if (logicalSources.size() > 1) {
            throw new LogicalSourceResolverException(
                    "Multiple logical sources found, but only one supported. Logical sources: %n%s"
                            .formatted(exception(logicalSources)));
        }

        if (logicalSources.isEmpty()) {
            throw new IllegalStateException("No logical sources registered");
        }

        var logicalSource = Iterables.getOnlyElement(logicalSources);

        if (resolvedSource == null || resolvedSource.getResolved().isEmpty()) {
            throw new LogicalSourceResolverException(
                    "No source provided for logical sources:%n%s".formatted(exception(logicalSources)));
        }

        var resolved = resolvedSource.getResolved().orElseThrow();

        var encoding = source.getEncoding();

        if (resolvedSource.getResolvedTypeRef().equals(new TypeRef<Mono<InputStream>>() {})) {
            return ((Mono<InputStream>) resolved)
                    .flatMapMany(inputStream -> getCsvRecordFlux(inputStream, encoding)
                            .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord)));
        } else if (resolved instanceof NamedCsvRecord resolvedRecord) {
            return Flux.just(LogicalSourceRecord.of(logicalSource, resolvedRecord));
        } else {
            throw new LogicalSourceResolverException(
                    "Unsupported source object provided for logical sources:%n%s".formatted(exception(logicalSources)));
        }
    }

    private Flux<NamedCsvRecord> getCsvRecordFlux(InputStream inputStream, IRI encoding) {
        var csvReaderBuilder = options.getCsvReaderBuilderFactory().get();

        Charset charset;
        if (source instanceof CsvwTable csvwTable) {
            var csvwDialect = csvwTable.getDialect();

            if (csvwDialect != null) {
                var dialectConfig = processDialect(csvwDialect);
                applyCsvwDialect(dialectConfig, csvReaderBuilder);
            }
            if (encoding != null) {
                charset = Encodings.resolveCharset(encoding)
                        .orElseThrow(() -> new LogicalSourceResolverException(
                                "Unsupported encoding provided: %s".formatted(encoding)));
            } else if (csvwDialect != null && csvwDialect.getEncoding() != null) {
                charset = Charset.forName(csvwDialect.getEncoding());
            } else {
                charset = UTF_8;
            }
        } else {
            charset = Encodings.resolveCharset(encoding).orElse(UTF_8);
        }

        return getCsvRecordFlux(csvReaderBuilder, inputStream, charset);
    }

    private Flux<NamedCsvRecord> getCsvRecordFlux(
            CsvReaderBuilder csvReaderBuilder, InputStream inputStream, Charset charset) {
        try {
            // Strip BOM before wrapping in InputStreamReader, since FastCSV's
            // detectBomHeader only works with Path-based input, not Reader.
            var bomFreeInputStream =
                    BOMInputStream.builder().setInputStream(inputStream).get();
            var flux = Flux.fromIterable(csvReaderBuilder.build(
                    buildNamedCsvRecordHandler(), new InputStreamReader(bomFreeInputStream, charset)));
            if (options.isStrictFieldCount()) {
                flux = flux.doOnNext(CsvResolver::validateFieldCount);
            }
            return flux;
        } catch (IOException ioException) {
            throw new LogicalSourceResolverException("Failed to create BOM-free input stream", ioException);
        }
    }

    private NamedCsvRecordHandler buildNamedCsvRecordHandler() {
        return NamedCsvRecordHandler.builder()
                .allowDuplicateHeaderFields(options.isAllowDuplicateHeaders())
                .build();
    }

    private static void validateFieldCount(NamedCsvRecord rec) {
        var headerSize = rec.getHeader().size();
        var fieldCount = rec.getFieldCount();
        if (fieldCount != headerSize) {
            throw new LogicalSourceResolverException(
                    "CSV record starting at line %d has %d field(s), but the header has %d"
                            .formatted(rec.getStartingLineNumber(), fieldCount, headerSize));
        }
    }

    private static CsvDialectConfig processDialect(io.carml.model.source.csvw.CsvwDialect csvwDialect) {
        try {
            return CsvwDialectProcessor.process(csvwDialect);
        } catch (CsvProcessingException csvProcessingException) {
            throw new LogicalSourceResolverException(csvProcessingException.getMessage(), csvProcessingException);
        }
    }

    private static void applyCsvwDialect(CsvDialectConfig config, CsvReaderBuilder csvReaderBuilder) {
        config.delimiter().ifPresent(csvReaderBuilder::fieldSeparator);
        config.quoteChar().ifPresent(csvReaderBuilder::quoteCharacter);
        config.commentPrefix()
                .ifPresentOrElse(
                        commentCharacter -> csvReaderBuilder
                                .commentCharacter(commentCharacter)
                                .commentStrategy(CommentStrategy.SKIP),
                        () -> csvReaderBuilder.commentStrategy(CommentStrategy.NONE));
    }

    @Override
    public LogicalSourceResolver.ExpressionEvaluationFactory<NamedCsvRecord> getExpressionEvaluationFactory() {
        var shouldTrim = source instanceof CsvwTable csvwTable
                && csvwTable.getDialect() != null
                && csvwTable.getDialect().trim();

        var nullValues = CsvNullValueHandler.resolveNullValues(source);

        return namedCsvRecord -> headerName -> {
            logEvaluateExpression(headerName, LOG);
            var result = namedCsvRecord.findField(headerName).orElse(null);

            if (shouldTrim && result != null) {
                result = result.trim();
            }

            if (result == null || nullValues.contains(result)) {
                return Optional.empty();
            }
            return Optional.of(result);
        };
    }

    @Override
    public Optional<Function<String, List<NamedCsvRecord>>> getInlineRecordParser() {
        return Optional.of(text -> {
            var records = new ArrayList<NamedCsvRecord>();
            options.getCsvReaderBuilderFactory()
                    .get()
                    .build(buildNamedCsvRecordHandler(), new StringReader(text))
                    .forEach(rec -> {
                        if (options.isStrictFieldCount()) {
                            validateFieldCount(rec);
                        }
                        records.add(rec);
                    });
            return List.copyOf(records);
        });
    }

    @Override
    public Optional<DatatypeMapperFactory<NamedCsvRecord>> getDatatypeMapperFactory() {
        return Optional.empty();
    }

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    @SuppressWarnings("unused")
    public static class Matcher implements MatchingLogicalSourceResolverFactory {

        @Override
        public Optional<MatchedLogicalSourceResolverFactory> apply(LogicalSource logicalSource) {
            var scoreBuilder = MatchedLogicalSourceResolverFactory.MatchScore.builder();

            if (matchesReferenceFormulation(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, CsvResolver.factory()));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() instanceof CsvReferenceFormulation;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
