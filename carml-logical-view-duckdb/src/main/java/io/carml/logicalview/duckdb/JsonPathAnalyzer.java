package io.carml.logicalview.duckdb;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.jsfr.json.compiler.JsonPathBaseVisitor;
import org.jsfr.json.compiler.JsonPathLexer;
import org.jsfr.json.compiler.JsonPathParser;

/**
 * Parses JSONPath expressions into structured components for SQL translation using JSurfer's
 * ANTLR-based parser.
 *
 * <p>The analyzer extracts:
 * <ul>
 *   <li>A <b>base path</b> with filters replaced by {@code [*]} wildcards</li>
 *   <li>Typed <b>filter conditions</b> that can be translated to SQL WHERE clauses</li>
 *   <li>A <b>deep scan</b> flag indicating whether the path uses recursive descent ({@code ..})</li>
 * </ul>
 *
 * <p>Compound filter expressions ({@code &&}, {@code ||}, {@code !}) are not supported and throw
 * {@link UnsupportedOperationException} at parse time. Simple (single-condition) filters are fully
 * supported.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class JsonPathAnalyzer {

    /**
     * Result of analyzing a JSONPath expression.
     *
     * @param basePath the path with filter expressions replaced by {@code [*]}
     * @param filters the extracted filter conditions (empty if no filters)
     * @param hasDeepScan whether the path uses recursive descent ({@code ..})
     */
    record ParsedJsonPath(String basePath, List<FilterCondition> filters, boolean hasDeepScan) {}

    /**
     * A typed filter condition extracted from a JSONPath filter expression. Each condition
     * references a field via its JSONPath (e.g., {@code $.age}) and a comparison value.
     */
    sealed interface FilterCondition {

        /** The JSONPath for the filtered field, e.g., {@code $.age}. */
        String fieldJsonPath();
    }

    record EqualStr(String fieldJsonPath, String value) implements FilterCondition {}

    record EqualNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record EqualBool(String fieldJsonPath, boolean value) implements FilterCondition {}

    record GreaterThanNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record LessThanNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record Exists(String fieldJsonPath) implements FilterCondition {}

    record MatchRegex(String fieldJsonPath, String pattern) implements FilterCondition {}

    /**
     * Analyzes a JSONPath expression and returns its structured components.
     *
     * @param jsonPath the JSONPath expression to analyze (e.g., {@code $.people[*]})
     * @return the parsed components
     * @throws UnsupportedOperationException if compound filter expressions are used
     */
    static ParsedJsonPath analyze(String jsonPath) {
        var lexer = new JsonPathLexer(CharStreams.fromString(jsonPath));
        var tokens = new CommonTokenStream(lexer);
        var parser = new JsonPathParser(tokens);
        var tree = parser.path();

        var visitor = new PathAnalysisVisitor();
        visitor.visit(tree);

        return new ParsedJsonPath(visitor.basePath.toString(), List.copyOf(visitor.filters), visitor.hasDeepScan);
    }

    /**
     * ANTLR visitor that walks the JSONPath parse tree to extract the base path, filter conditions,
     * and deep scan flag. The default {@link JsonPathBaseVisitor#visitChildren} implementation
     * delegates to child-specific visit methods, so overriding the leaf visit methods is sufficient.
     */
    private static class PathAnalysisVisitor extends JsonPathBaseVisitor<Void> {

        private final StringBuilder basePath = new StringBuilder("$");
        private final List<FilterCondition> filters = new ArrayList<>();
        private boolean hasDeepScan = false;

        @Override
        public Void visitChildNode(JsonPathParser.ChildNodeContext ctx) {
            basePath.append('.').append(ctx.KEY().getText());
            return null;
        }

        @Override
        public Void visitAnyIndex(JsonPathParser.AnyIndexContext ctx) {
            basePath.append("[*]");
            return null;
        }

        @Override
        public Void visitIndex(JsonPathParser.IndexContext ctx) {
            basePath.append('[').append(ctx.NUM().getText()).append(']');
            return null;
        }

        @Override
        public Void visitAny(JsonPathParser.AnyContext ctx) {
            basePath.append("[*]");
            return null;
        }

        @Override
        public Void visitIndexes(JsonPathParser.IndexesContext ctx) {
            basePath.append("[*]");
            return null;
        }

        @Override
        public Void visitSlicing(JsonPathParser.SlicingContext ctx) {
            basePath.append("[*]");
            return null;
        }

        @Override
        public Void visitChildrenNode(JsonPathParser.ChildrenNodeContext ctx) {
            basePath.append("[*]");
            return null;
        }

        @Override
        public Void visitAnyChild(JsonPathParser.AnyChildContext ctx) {
            basePath.append(".*");
            return null;
        }

        @Override
        public Void visitSearch(JsonPathParser.SearchContext ctx) {
            hasDeepScan = true;
            basePath.append("..");
            return null;
        }

        @Override
        public Void visitSearchChild(JsonPathParser.SearchChildContext ctx) {
            hasDeepScan = true;
            basePath.append("..").append(ctx.KEY().getText());
            return null;
        }

        @Override
        public Void visitFilter(JsonPathParser.FilterContext ctx) {
            basePath.append("[*]");
            var filterExpr = ctx.filterExpr();
            if (filterExpr != null) {
                filters.add(extractFilterCondition(filterExpr));
            }
            return null;
        }

        private static FilterCondition extractFilterCondition(JsonPathParser.FilterExprContext ctx) {
            if (ctx.filterGtNum() != null) {
                return extractGreaterThan(ctx.filterGtNum());
            }
            if (ctx.filterLtNum() != null) {
                return extractLessThan(ctx.filterLtNum());
            }
            if (ctx.filterEqualNum() != null) {
                return extractEqualNum(ctx.filterEqualNum());
            }
            if (ctx.filterEqualStr() != null) {
                return extractEqualStr(ctx.filterEqualStr());
            }
            if (ctx.filterEqualBool() != null) {
                return extractEqualBool(ctx.filterEqualBool());
            }
            if (ctx.filterExist() != null) {
                return extractExists(ctx.filterExist());
            }
            if (ctx.filterMatchRegex() != null) {
                return extractMatchRegex(ctx.filterMatchRegex());
            }

            throw new UnsupportedOperationException(
                    "Compound JSONPath filter expressions (&&, ||, !) are not yet implemented in JsonPathAnalyzer");
        }

        /**
         * Builds a JSONPath string from the {@code relativePath} nodes inside a filter expression.
         * Converts the filter-relative path (e.g., {@code @.name}) to a root-relative path (e.g.,
         * {@code $.name}).
         */
        private static String buildFilterFieldPath(List<JsonPathParser.RelativePathContext> paths) {
            var sb = new StringBuilder("$");
            for (var rp : paths) {
                if (rp.childNode() != null) {
                    sb.append('.').append(rp.childNode().KEY().getText());
                } else if (rp.anyIndex() != null) {
                    sb.append("[*]");
                } else if (rp.index() != null) {
                    sb.append('[').append(rp.index().NUM().getText()).append(']');
                }
            }
            return sb.toString();
        }

        private static GreaterThanNum extractGreaterThan(JsonPathParser.FilterGtNumContext ctx) {
            return new GreaterThanNum(
                    buildFilterFieldPath(ctx.relativePath()),
                    new BigDecimal(ctx.NUM().getText()));
        }

        private static LessThanNum extractLessThan(JsonPathParser.FilterLtNumContext ctx) {
            return new LessThanNum(
                    buildFilterFieldPath(ctx.relativePath()),
                    new BigDecimal(ctx.NUM().getText()));
        }

        private static EqualNum extractEqualNum(JsonPathParser.FilterEqualNumContext ctx) {
            return new EqualNum(
                    buildFilterFieldPath(ctx.relativePath()),
                    new BigDecimal(ctx.NUM().getText()));
        }

        private static EqualStr extractEqualStr(JsonPathParser.FilterEqualStrContext ctx) {
            var raw = ctx.QUOTED_STRING().getText();
            var value = raw.substring(1, raw.length() - 1);
            return new EqualStr(buildFilterFieldPath(ctx.relativePath()), value);
        }

        private static EqualBool extractEqualBool(JsonPathParser.FilterEqualBoolContext ctx) {
            return new EqualBool(
                    buildFilterFieldPath(ctx.relativePath()),
                    Boolean.parseBoolean(ctx.BOOL().getText()));
        }

        private static Exists extractExists(JsonPathParser.FilterExistContext ctx) {
            return new Exists(buildFilterFieldPath(ctx.relativePath()));
        }

        private static MatchRegex extractMatchRegex(JsonPathParser.FilterMatchRegexContext ctx) {
            var raw = ctx.REGEX().getText();
            var lastSlash = raw.lastIndexOf('/');
            var pattern = raw.substring(1, lastSlash);
            return new MatchRegex(buildFilterFieldPath(ctx.relativePath()), pattern);
        }
    }
}
