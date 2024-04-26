package io.carml.logicalsourceresolver;

import static io.carml.util.LogUtil.exception;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Iterables;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvReader.CsvReaderBuilder;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.carml.logicalsourceresolver.sourceresolver.Encodings;
import io.carml.logicalsourceresolver.sourceresolver.GetHttpUrl;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwDialect;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.vocab.Rdf.Ql;
import io.carml.vocab.Rdf.Rml;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvResolver implements LogicalSourceResolver<NamedCsvRecord> {

    public static LogicalSourceResolverFactory<NamedCsvRecord> factory() {
        return CsvResolver::new;
    }

    private final Source source;

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<NamedCsvRecord>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSourceFilter) {
        return resolvedSource -> getCsvRecordFlux(resolvedSource, logicalSourceFilter);
    }

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

        var resolved = resolvedSource.getResolved().get();

        var charset = Encodings.resolveCharset(resolvedSource.getRmlSource().getEncoding())
                .orElse(UTF_8);

        if (resolved instanceof InputStream resolvedInputStream) {
            return getCsvRecordFlux(resolvedInputStream, charset)
                    .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
        } else if (resolved instanceof CsvwTable csvwTable) {
            return getCsvwTableRecordFlux(csvwTable, charset)
                    .map(lsRecord -> LogicalSourceRecord.of(logicalSource, lsRecord));
        } else if (resolved instanceof NamedCsvRecord resolvedRecord) {
            return Flux.just(LogicalSourceRecord.of(logicalSource, resolvedRecord));
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<NamedCsvRecord> getCsvwTableRecordFlux(CsvwTable csvwTable, Charset charset) {
        var csvwDialect = csvwTable.getDialect();
        var csvReaderBuilder = CsvReader.builder().detectBomHeader(true);

        applyCsvwDialect(csvwDialect, csvReaderBuilder);

        var charsetToUse = csvwDialect.getEncoding() != null ? Charset.forName(csvwDialect.getEncoding()) : charset;
        return GetHttpUrl.getInstance()
                .apply(GetHttpUrl.toUrl(csvwTable.getUrl()))
                .flatMapMany(inputStream -> getCsvRecordFlux(csvReaderBuilder, inputStream, charsetToUse));
    }

    private void applyCsvwDialect(CsvwDialect csvwDialect, CsvReaderBuilder csvReaderBuilder) {
        applyDelimiter(csvwDialect, csvReaderBuilder);
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

    private Flux<NamedCsvRecord> getCsvRecordFlux(InputStream inputStream, Charset charset) {
        var csvReaderBuilder = CsvReader.builder().detectBomHeader(true);

        return getCsvRecordFlux(csvReaderBuilder, inputStream, charset);
    }

    private Flux<NamedCsvRecord> getCsvRecordFlux(
            CsvReaderBuilder csvReaderBuilder, InputStream inputStream, Charset charset) {
        return Flux.fromStream(csvReaderBuilder.ofNamedCsvRecord(new InputStreamReader(inputStream, charset)).stream());
    }

    @Override
    public LogicalSourceResolver.ExpressionEvaluationFactory<NamedCsvRecord> getExpressionEvaluationFactory() {
        return namedCsvRecord -> headerName -> {
            logEvaluateExpression(headerName, LOG);
            var result = namedCsvRecord.getField(headerName);
            if (result == null || result.isEmpty()) {
                return Optional.empty();
            }
            return Optional.ofNullable(namedCsvRecord.getField(headerName));
        };
    }

    @Override
    public Optional<DatatypeMapperFactory<NamedCsvRecord>> getDatatypeMapperFactory() {
        return Optional.empty();
    }

    @ToString
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Matcher implements MatchingLogicalSourceResolverFactory {
        private static final Set<IRI> MATCHING_REF_FORMULATIONS = Set.of(Rml.Csv, Ql.Csv);

        private List<IRI> matchingReferenceFormulations;

        public static Matcher getInstance() {
            return getInstance(Set.of());
        }

        public static Matcher getInstance(Set<IRI> customMatchingReferenceFormulations) {
            return new Matcher(
                    Stream.concat(customMatchingReferenceFormulations.stream(), MATCHING_REF_FORMULATIONS.stream())
                            .distinct()
                            .toList());
        }

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
            return logicalSource.getReferenceFormulation() != null
                    && matchingReferenceFormulations.contains(logicalSource.getReferenceFormulation());
        }

        @Override
        public String getResolverName() {
            return "CsvResolver";
        }
    }
}
