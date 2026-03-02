package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.service.AutoService;
import com.google.common.collect.Iterables;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvReader.CsvReaderBuilder;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.carml.logicalsourceresolver.sourceresolver.Encodings;
import io.carml.model.CsvReferenceFormulation;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwDialect;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.util.TypeRef;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.BOMInputStream;
import org.eclipse.rdf4j.model.IRI;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvResolver implements LogicalSourceResolver<NamedCsvRecord> {

    public static final String NAME = "CsvResolver";

    public static LogicalSourceResolverFactory<NamedCsvRecord> factory() {
        return CsvResolver::new;
    }

    private final Source source;

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<NamedCsvRecord>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSourceFilter) {
        return resolvedSource -> getCsvRecordFlux(resolvedSource, logicalSourceFilter);
    }

    @SuppressWarnings("unchecked")
    private Flux<LogicalSourceRecord<NamedCsvRecord>> getCsvRecordFlux(
            ResolvedSource<?> resolvedSource, Set<LogicalSource> logicalSources) {
        if (logicalSources.size() > 1) {
            throw new LogicalSourceResolverException(String.format(
                    "Multiple logical sources found, but only one supported. Logical sources: %n%s",
                    exception(logicalSources)));
        }

        if (logicalSources.isEmpty()) {
            throw new IllegalStateException("No logical sources registered");
        }

        var logicalSource = Iterables.getOnlyElement(logicalSources);

        if (resolvedSource == null || resolvedSource.getResolved().isEmpty()) {
            throw new LogicalSourceResolverException(
                    String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
        }

        var resolved = resolvedSource.getResolved().orElseThrow();

        var encoding = source.getEncoding();

        if (resolvedSource.getResolvedTypeRef().equals(new TypeRef<Mono<InputStream>>() {})) {
            return ((Mono<InputStream>) resolved).flatMapMany(inputStream -> getCsvRecordFlux(inputStream, encoding)
                    .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord)));
        } else if (resolved instanceof NamedCsvRecord resolvedRecord) {
            return Flux.just(LogicalSourceRecord.of(logicalSource, resolvedRecord));
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<NamedCsvRecord> getCsvRecordFlux(InputStream inputStream, IRI encoding) {
        var csvReaderBuilder = CsvReader.builder();

        Charset charset;
        if (source instanceof CsvwTable csvwTable) {
            var csvwDialect = csvwTable.getDialect();

            if (csvwDialect != null) {
                applyCsvwDialect(csvwDialect, csvReaderBuilder);
            }
            if (encoding != null) {
                charset = Encodings.resolveCharset(encoding)
                        .orElseThrow(() -> new LogicalSourceResolverException(
                                String.format("Unsupported encoding provided: %s", encoding)));
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
            return Flux.fromIterable(
                    csvReaderBuilder.ofNamedCsvRecord(new InputStreamReader(bomFreeInputStream, charset)));
        } catch (IOException ioException) {
            throw new LogicalSourceResolverException("Failed to create BOM-free input stream", ioException);
        }
    }

    private void applyCsvwDialect(CsvwDialect csvwDialect, CsvReaderBuilder csvReaderBuilder) {
        applyDelimiter(csvwDialect, csvReaderBuilder);
        applyQuoteCharacter(csvwDialect, csvReaderBuilder);
        applyCommentStrategy(csvwDialect, csvReaderBuilder);
    }

    private void applyDelimiter(CsvwDialect csvwDialect, CsvReaderBuilder csvReaderBuilder) {
        toChar(csvwDialect.getDelimiter(), "CSVW delimiter").ifPresent(csvReaderBuilder::fieldSeparator);
    }

    private void applyQuoteCharacter(CsvwDialect csvwDialect, CsvReaderBuilder csvReaderBuilder) {
        toChar(csvwDialect.getQuoteChar(), "CSVW quote character").ifPresent(csvReaderBuilder::quoteCharacter);
    }

    private void applyCommentStrategy(CsvwDialect csvwDialect, CsvReaderBuilder csvReaderBuilder) {
        toChar(csvwDialect.getCommentPrefix(), "CSVW comment prefix")
                .ifPresentOrElse(
                        commentCharacter -> csvReaderBuilder
                                .commentCharacter(commentCharacter)
                                .commentStrategy(CommentStrategy.SKIP),
                        () -> csvReaderBuilder.commentStrategy(CommentStrategy.NONE));
    }

    private Optional<Character> toChar(String string, String errorSubject) {
        if (string == null || string.isEmpty()) {
            return Optional.empty();
        }
        if (string.length() > 1) {
            throw new LogicalSourceResolverException(
                    String.format("%s must be a single character, but was %s", errorSubject, string));
        }

        return Optional.of(string.charAt(0));
    }

    @Override
    public LogicalSourceResolver.ExpressionEvaluationFactory<NamedCsvRecord> getExpressionEvaluationFactory() {
        var shouldTrim = source instanceof CsvwTable csvwTable
                && csvwTable.getDialect() != null
                && csvwTable.getDialect().trim();

        var nullValues = getNullValues();

        return namedCsvRecord -> headerName -> {
            logEvaluateExpression(headerName, LOG);
            var result = namedCsvRecord.getField(headerName);

            if (shouldTrim && result != null) {
                result = result.trim();
            }

            if (result == null || nullValues.contains(result)) {
                return Optional.empty();
            }
            return Optional.of(result);
        };
    }

    private Set<Object> getNullValues() {
        var rmlNulls = source.getNulls();
        if (source instanceof CsvwTable csvwTable) {
            var csvwNulls = csvwTable.getCsvwNulls();
            if (csvwNulls != null && !csvwNulls.isEmpty()) {
                var merged = new HashSet<>(rmlNulls != null ? rmlNulls : Set.of());
                merged.addAll(csvwNulls);
                return Set.copyOf(merged);
            }
        }
        return rmlNulls != null ? rmlNulls : Set.of();
    }

    @Override
    public Optional<Function<String, List<NamedCsvRecord>>> getInlineRecordParser() {
        return Optional.of(text -> {
            var records = new ArrayList<NamedCsvRecord>();
            CsvReader.builder().ofNamedCsvRecord(new StringReader(text)).forEach(records::add);
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
