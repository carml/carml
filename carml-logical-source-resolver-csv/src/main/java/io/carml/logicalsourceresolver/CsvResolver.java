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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
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

        var resolved = resolvedSource.getResolved().get();

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
        var csvReaderBuilder = CsvReader.builder().detectBomHeader(true);

        Charset charset;
        if (source instanceof CsvwTable csvwTable) {
            var csvwDialect = csvwTable.getDialect();

            applyCsvwDialect(csvwDialect, csvReaderBuilder);
            if (encoding != null) {
                charset = Encodings.resolveCharset(encoding)
                        .orElseThrow(() -> new LogicalSourceResolverException(
                                String.format("Unsupported encoding provided: %s", encoding)));
            } else if (csvwDialect.getEncoding() != null) {
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
        return Flux.fromStream(csvReaderBuilder.ofNamedCsvRecord(new InputStreamReader(inputStream, charset)).stream());
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
        return namedCsvRecord -> headerName -> {
            logEvaluateExpression(headerName, LOG);
            var result = namedCsvRecord.getField(headerName);

            if (result == null || source.getNulls().contains(result)) {
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
    @AutoService(MatchingLogicalSourceResolverFactory.class)
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
