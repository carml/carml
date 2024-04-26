package io.carml.logicalsourceresolver.sql;

import static io.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.ChildMap;
import io.carml.model.DatabaseSource;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.ParentMap;
import io.carml.model.RefObjectMap;
import io.carml.model.Source;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.eclipse.rdf4j.model.IRI;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class SqlResolver implements LogicalSourceResolver<RowData> {

    public static final String CHILD_ALIAS = "child";

    public static final String PARENT_ALIAS = "parent";

    private final Source source;

    private final boolean isStrict;

    SqlResolver(Source source, boolean isStrict) {
        this.source = source;
        this.isStrict = isStrict;
    }

    @Override
    public Function<ResolvedSource<?>, Flux<LogicalSourceRecord<RowData>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources) {
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

    private ConnectionFactory getPooledConnectionFactory(ConnectionFactory connectionFactory, int size) {
        var poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
                // .initialSize(size)
                .initialSize(size)
                .maxSize(size * 2)
                // .maxIdleTime(null)
                // .maxIdleTime(Duration.ZERO)
                // .maxSize(100)
                .build();

        return new ConnectionPool(poolConfiguration);
    }

    private Flux<LogicalSourceRecord<RowData>> getResultFlux(
            ConnectionFactory connectionFactory, Set<LogicalSource> logicalSources) {
        var logicalSourcesGroupedByQuery =
                logicalSources.stream().collect(groupingBy(this::getSelectQueryString, toUnmodifiableSet()));

        // return Flux.merge(logicalSourcesGroupedByQuery.entrySet()
        // .stream()
        // .map(queryEntry -> getResultFluxForQueryString(
        // /* getPooledConnectionFactory( */connectionFactory/* , logicalSources.size()) */,
        // queryEntry.getKey(),
        // queryEntry.getValue()))
        // .collect(toUnmodifiableSet()));

        return Flux.fromIterable(logicalSourcesGroupedByQuery.entrySet())
                .concatMap(queryEntry -> getResultFluxForQueryString(
                        /* getPooledConnectionFactory( */ connectionFactory /* , logicalSources.size()) */,
                        queryEntry.getKey(),
                        queryEntry.getValue()));
    }

    private String getSelectQueryString(LogicalSource logicalSource) {
        var source = logicalSource.getSource();
        if (source instanceof JoiningDatabaseSource joiningDatabaseSource) {
            return getJointSqlQuery(joiningDatabaseSource);
        } else if (logicalSource.getSource() instanceof DatabaseSource) {
            if (logicalSource.getQuery() != null) {
                return logicalSource.getQuery();
            } else if (logicalSource.getTableName() != null) {
                return getQuery(logicalSource);
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
        // if (LOG.isDebugEnabled()) {
        LOG.info("Executing query:");
        LOG.info("{}", selectQuery);
        // }

        // return Flux.merge(logicalSources.stream()
        // .map(logicalSource -> Mono.from(connectionFactory.create())
        // .flatMapMany(connection -> {
        // var statement = connection.createStatement(selectQuery);
        //
        // return Mono.from(statement.execute())
        // .flatMapMany(result -> result.map(this::toRowData))
        // .map(pair -> LogicalSourceRecord.of(logicalSource, pair))
        // .concatWith(Flux.from(Mono.from(connection.close())
        // .doOnError(error -> LOG.error("Connection close failed: {}", error.getMessage()))
        // .doOnSuccess(success -> LOG.info("Successfully closed connection."))
        // .map(obj -> LogicalSourceRecord.of(logicalSource, RowData.of(Map.of(), Map.of())))))
        // .doOnCancel(() -> {
        // LOG.info("Cancelling... Closing connection...");
        // Mono.from(connection.close())
        // .doOnError(error -> LOG.error("Connection close failed: {}", error.getMessage()))
        // .doOnSuccess(success -> LOG.info("Successfully closed connection."))
        // .subscribe();
        // })
        // .log("query");
        // }))
        // .collect(toUnmodifiableSet()));

        return Flux.fromIterable(logicalSources).concatMap(logicalSource -> Mono.from(connectionFactory.create())
                .flatMapMany(connection -> {
                    var statement = connection.createStatement(selectQuery);

                    return Mono.from(statement.execute())
                            .flatMapMany(result -> result.map(this::toRowData))
                            .map(pair -> LogicalSourceRecord.of(logicalSource, pair))
                            .concatWith(Flux.from(Mono.from(connection.close())
                                    .doOnError(error -> LOG.error("Connection close failed: {}", error.getMessage()))
                                    .doOnSuccess(success -> LOG.info("Successfully closed connection."))
                                    .map(obj -> LogicalSourceRecord.of(logicalSource, RowData.of()))))
                            .doOnCancel(() -> {
                                LOG.info("Cancelling... Closing connection...");
                                Mono.from(connection.close())
                                        .doOnError(
                                                error -> LOG.error("Connection close failed: {}", error.getMessage()))
                                        .doOnSuccess(success -> LOG.info("Successfully closed connection."))
                                        .subscribe();
                            })
                            .doFinally(signalType -> {
                                LOG.info("Finally: {}", signalType);
                                LOG.info("Closing connection...");
                                Mono.from(connection.close())
                                        .doOnError(
                                                error -> LOG.error("Connection close failed: {}", error.getMessage()))
                                        .doOnSuccess(success -> LOG.info("Successfully closed connection."))
                                        .subscribe();
                            });
                }));

        // return Flux.fromIterable(logicalSources)
        // .concatMap(logicalSource -> Flux.usingWhen(connectionFactory.create(), c ->
        // c.createStatement(selectQuery)
        // .execute(), Connection::close)
        // .concatMap(result -> result.map(this::toRowData))
        // .map(pair -> LogicalSourceRecord.of(logicalSource, pair)));
    }

    private Mono<Void> closeConnection(Connection connection) {
        LOG.info("Closing connection...");
        return Mono.from(connection.close())
                .doOnError(error -> LOG.error("Connection close failed: {}", error.getMessage()))
                .doOnSuccess(success -> LOG.info("Successfully closed connection."));
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

    // private static DSLContext asteriskReplacingDslContext(SQLDialect sqlDialect,
    // SelectAsteriskReplacer selectAsteriskReplacer) {
    // var configuration = new DefaultConfiguration().set(sqlDialect)
    // .set(selectAsteriskReplacer);
    //
    // return DSL.using(configuration);
    // }

    public abstract String getQuery(LogicalSource logicalSource);

    public static String getQuery(SQLDialect sqlDialect, LogicalSource logicalSource) {
        var fields = logicalSource.getExpressions().stream()
                .map(expr -> field(name(expr)))
                .collect(toUnmodifiableSet());

        var sql = getSelect(DSL.using(sqlDialect), logicalSource, fields).getSQL();

        return sql;
    }

    public abstract String getJointSqlQuery(JoiningDatabaseSource joiningDatabaseSourceSupplier);

    public static String getJointSqlQuery(SQLDialect sqlDialect, JoiningDatabaseSource joiningDatabaseSourceSupplier) {

        var childTmExpressions = joiningDatabaseSourceSupplier.getChildExpressions();
        var childProjection = getFields(childTmExpressions, null, null);
        var qualifiedChildProjection = getFields(childTmExpressions, CHILD_ALIAS, null);

        var parentTmExpressions = joiningDatabaseSourceSupplier.getParentExpressions();
        var parentProjection = getFields(parentTmExpressions, null, null);
        var qualifiedParentProjection = getFields(parentTmExpressions, PARENT_ALIAS, PARENT_ALIAS);

        var ctx = DSL.using(sqlDialect);

        var childSelect = getSelect(ctx, joiningDatabaseSourceSupplier.getChildLogicalSource(), childProjection);
        var parentSelect = getSelect(ctx, joiningDatabaseSourceSupplier.getParentLogicalSource(), parentProjection);

        var refObjectMaps = joiningDatabaseSourceSupplier.getRefObjectMaps();

        childSelect.addConditions(toChildNotNullConditions(refObjectMaps));
        var childTable = childSelect.asTable().as(CHILD_ALIAS);

        // TODO: is it safe to add parentTmExpressions?
        parentSelect.addConditions(toParentNotNullConditions(refObjectMaps, parentTmExpressions));
        var parentTable = parentSelect.asTable().as(PARENT_ALIAS);

        var projection = Stream.concat(qualifiedChildProjection.stream(), qualifiedParentProjection.stream())
                .toList();

        return ctx.select(projection)
                .from(childTable)
                .innerJoin(parentTable)
                .on(toJoinConditions(refObjectMaps))
                .getSQL();
    }

    private static Set<Field<Object>> getFields(Set<String> expressions, String qualifier, String alias) {
        return expressions.stream()
                .map(expr -> qualifier != null ? field(name(qualifier, expr)) : field(name(expr)))
                .map(field -> alias != null ? field.as(String.format("%s.%s", alias, field.getName())) : field)
                .collect(toUnmodifiableSet());
    }

    private static SelectQuery<?> getSelect(
            DSLContext ctx, LogicalSource logicalSource, Set<Field<Object>> projection) {
        if (logicalSource.getQuery() != null) {
            return (SelectQuery<?>) ctx.parser().parseSelect(logicalSource.getQuery());
        } else {
            return ctx.select(projection).from(logicalSource.getTableName()).getQuery();
        }
    }

    private static Condition[] toChildNotNullConditions(Set<RefObjectMap> refObjectMaps) {
        var childJoinExpressions = refObjectMaps.stream()
                .map(RefObjectMap::getJoinConditions)
                .flatMap(Set::stream)
                .map(Join::getChildMap)
                .map(ChildMap::getExpressionMapExpressionSet) // TODO template and functionvalue?
                .flatMap(Set::stream)
                .collect(toSet());

        return toNotNullConditions(childJoinExpressions, Set.of());
    }

    private static Condition[] toParentNotNullConditions(Set<RefObjectMap> refObjectMaps, Set<String> extraFields) {
        var parentJoinExpressions = refObjectMaps.stream()
                .map(RefObjectMap::getJoinConditions)
                .flatMap(Set::stream)
                .map(Join::getParentMap)
                .map(ParentMap::getExpressionMapExpressionSet) // TODO template and functionvalue?
                .flatMap(Set::stream)
                .collect(toSet());

        return toNotNullConditions(parentJoinExpressions, extraFields);
    }

    private static Condition[] toNotNullConditions(Set<String> joiningFields, Set<String> extraFields) {
        return Stream.concat(joiningFields.stream(), extraFields.stream())
                .distinct()
                .map(field -> field(name(field)).isNotNull())
                .toArray(Condition[]::new);
    }

    private static Condition[] toJoinConditions(Set<RefObjectMap> refObjectMaps) {
        return refObjectMaps.stream()
                .map(RefObjectMap::getJoinConditions)
                .flatMap(Set::stream)
                .map(join -> field(name(CHILD_ALIAS, join.getChildMap().getReference()))
                        .eq(field(name(PARENT_ALIAS, join.getParentMap().getReference())))) // TODO other expressions?
                .toArray(Condition[]::new);
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

            return result == null ? Optional.empty() : Optional.of(result);
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
