package io.carml.logicalsourceresolver.sql;

import static io.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.eclipse.rdf4j.model.IRI;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class SqlResolver implements LogicalSourceResolver<RowData> {

    static {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
        System.getProperties().setProperty("org.jooq.no-tips", "true");
    }

    private final Source source;

    private final boolean isStrict;

    private Map<LogicalSource, Set<String>> expressionsPerLogicalSource = Map.of();

    SqlResolver(Source source, boolean isStrict) {
        this.source = source;
        this.isStrict = isStrict;
    }

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<RowData>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources) {
        return resolvedSource -> getLogicalSourceRecordFlux(resolvedSource, logicalSources);
    }

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<RowData>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources, Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {
        this.expressionsPerLogicalSource = expressionsPerLogicalSource;
        return resolvedSource -> getLogicalSourceRecordFlux(resolvedSource, logicalSources);
    }

    private Flux<LogicalSourceRecord<RowData>> getLogicalSourceRecordFlux(
            ResolvedSource<?> resolvedSource, Set<LogicalSource> logicalSources) {

        if (logicalSources.isEmpty()) {
            throw new IllegalStateException("No logical sources registered");
        }

        if (resolvedSource == null || resolvedSource.getResolved().isEmpty()) {
            throw new LogicalSourceResolverException(
                    String.format("No source provided for logical sources:%n%s", exception(logicalSources)));
        }

        var resolved = resolvedSource.getResolved().get();

        if (resolved instanceof ConnectionFactoryOptions connectionFactoryOptions) {
            var connectionFactory = ConnectionFactories.get(connectionFactoryOptions);

            return getResultFlux(connectionFactory, logicalSources);
        } else {
            throw new LogicalSourceResolverException(String.format(
                    "Unsupported source object provided for logical sources:%n%s", exception(logicalSources)));
        }
    }

    private Flux<LogicalSourceRecord<RowData>> getResultFlux(
            ConnectionFactory connectionFactory, Set<LogicalSource> logicalSources) {
        var logicalSourcesGroupedByQuery =
                logicalSources.stream().collect(groupingBy(this::getSelectQueryString, toUnmodifiableSet()));

        return Flux.fromIterable(logicalSourcesGroupedByQuery.entrySet())
                .concatMap(queryEntry ->
                        getResultFluxForQueryString(connectionFactory, queryEntry.getKey(), queryEntry.getValue()));
    }

    private String getSelectQueryString(LogicalSource logicalSource) {
        if (logicalSource.getSource() instanceof DatabaseSource) {
            if (logicalSource.getQuery() != null) {
                return logicalSource.getQuery();
            } else if (logicalSource.getTableName() != null) {
                return getQuery(logicalSource, expressionsPerLogicalSource.getOrDefault(logicalSource, Set.of()));
            } else {
                throw new LogicalSourceResolverException(
                        String.format("No query or table name found for logical source %s", exception(logicalSource)));
            }
        } else {
            // TODO
            throw new LogicalSourceResolverException("");
        }
    }

    private Flux<LogicalSourceRecord<RowData>> getResultFluxForQueryString(
            ConnectionFactory connectionFactory, String selectQuery, Set<LogicalSource> logicalSources) {
        LOG.debug("Executing query:");
        LOG.debug("{}", selectQuery);

        return Flux.fromIterable(logicalSources).concatMap(logicalSource -> Flux.usingWhen(
                        connectionFactory.create(),
                        connection -> Mono.from(
                                        connection.createStatement(selectQuery).execute())
                                .flatMapMany(result -> result.map(this::toRowData)),
                        this::closeConnection)
                .map(rowData -> LogicalSourceRecord.of(logicalSource, rowData)));
    }

    private Mono<Void> closeConnection(Connection connection) {
        LOG.debug("Closing connection...");
        return Mono.from(connection.close())
                .doOnError(error -> LOG.error("Connection close failed: {}", error.getMessage()))
                .doOnSuccess(success -> LOG.debug("Successfully closed connection."));
    }

    private RowData toRowData(Row row, RowMetadata rowMetadata) {
        var columnMetadatas = rowMetadata.getColumnMetadatas();

        var data = new CaseInsensitiveMap<String, Object>(columnMetadatas.size());
        var columnTypes = new CaseInsensitiveMap<String, Type>(columnMetadatas.size());

        IntStream.range(0, columnMetadatas.size()).forEach(index -> {
            var columnMetadata = columnMetadatas.get(index);
            data.put(columnMetadata.getName(), row.get(index));
            columnTypes.put(columnMetadata.getName(), columnMetadata.getType());
        });

        return RowData.of(data, columnTypes);
    }

    public abstract String getQuery(LogicalSource logicalSource, Set<String> expressions);

    public static String getQuery(SQLDialect sqlDialect, LogicalSource logicalSource, Set<String> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return DSL.using(sqlDialect)
                    .select()
                    .from(logicalSource.getTableName())
                    .getSQL();
        }

        var fields = expressions.stream().map(expr -> field(name(expr))).collect(toUnmodifiableSet());

        return getSelect(DSL.using(sqlDialect), logicalSource, fields).getSQL();
    }

    private static SelectQuery<?> getSelect(
            DSLContext ctx, LogicalSource logicalSource, Set<Field<Object>> projection) {
        if (logicalSource.getQuery() != null) {
            return (SelectQuery<?>) ctx.parser().parseSelect(logicalSource.getQuery());
        } else {
            return ctx.select(projection).from(logicalSource.getTableName()).getQuery();
        }
    }

    @Override
    public ExpressionEvaluationFactory<RowData> getExpressionEvaluationFactory() {
        return rowData -> expression -> {
            var data = rowData.getData();

            if (isStrict && !data.containsKey(expression)) {
                throw new LogicalSourceResolverException(
                        String.format("Could not resolve column %s in %n%s", expression, data));
            }

            var result = data.get(expression);

            return result == null || source.getNulls().contains(result) ? Optional.empty() : Optional.of(result);
        };
    }

    @Override
    public Optional<DatatypeMapperFactory<RowData>> getDatatypeMapperFactory() {
        return Optional.of(rowData -> expression -> {
            var columnTypes = rowData.getColumnTypes();

            if (isStrict && !columnTypes.containsKey(expression)) {
                throw new LogicalSourceResolverException(
                        String.format("Could not resolve column %s in %n%s", expression, columnTypes));
            }

            var type = columnTypes.get(expression);

            return type == null ? Optional.empty() : Optional.of(getDatatypeIri(type));
        });
    }

    // https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
    public abstract IRI getDatatypeIri(Type sqlDataType);
}
