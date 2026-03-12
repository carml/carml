package io.carml.logicalview.duckdb;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;
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
 * <p>Both simple (single-condition) and compound filter expressions ({@code &&}, {@code ||},
 * {@code !}) are supported.
 *
 * <p>The JSurfer ANTLR grammar does not support the {@code !=}, {@code <=}, and {@code >=}
 * comparison operators. Before parsing, a pre-processing step rewrites these operators as negated
 * equivalents: {@code !=} becomes {@code !(... == ...)}, {@code >=} becomes {@code !(... < ...)},
 * and {@code <=} becomes {@code !(... > ...)}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class JsonPathAnalyzer {

    /** Matches {@code !=}, {@code >=}, or {@code <=} with surrounding context in a filter expression. */
    private static final Pattern UNSUPPORTED_OP = Pattern.compile("(!=|>=|<=)");

    private static final Map<String, String> OP_REPLACEMENTS = Map.of("!=", "==", ">=", "<", "<=", ">");

    /**
     * Matches three-part array slice expressions ({@code [start:end:step]}) where the step parameter
     * is not supported by the JSurfer ANTLR grammar. Start and end may be empty; step must be
     * present as a non-negative integer.
     */
    private static final Pattern THREE_PART_SLICE = Pattern.compile("\\[(-?\\d*):(-?\\d*):(-?\\d+)]");

    /**
     * Result of analyzing a JSONPath expression.
     *
     * @param basePath the path with filter expressions replaced by {@code [*]}
     * @param filters the extracted filter conditions (empty if no filters)
     * @param hasDeepScan whether the path uses recursive descent ({@code ..})
     * @param slices the slice selectors encountered in the path (empty if no slices)
     * @param unions the union selectors encountered in the path (empty if no unions)
     */
    record ParsedJsonPath(
            String basePath,
            List<FilterCondition> filters,
            boolean hasDeepScan,
            List<SliceSelector> slices,
            List<UnionSelector> unions) {}

    /**
     * A union selector extracted from a JSONPath index union ({@code [0,2,5]}) or name union
     * ({@code ['name','age']}) expression.
     */
    sealed interface UnionSelector {}

    /**
     * An index union selector that selects specific array elements by their 0-based indices.
     *
     * @param indices the list of 0-based array indices to select
     */
    record IndexUnion(List<Integer> indices) implements UnionSelector {}

    /**
     * A name union selector that selects specific object keys by name.
     *
     * @param names the list of object key names to select
     */
    record NameUnion(List<String> names) implements UnionSelector {}

    /**
     * A slice selector extracted from a JSONPath array slice expression ({@code [start:end:step]}).
     *
     * <p>Both bounds follow JSONPath semantics: 0-based, start-inclusive, end-exclusive. Either bound
     * may be {@code null} to indicate "from beginning" or "to end". The step selects every
     * <i>n</i>th element within the range.
     *
     * @param start the 0-based inclusive start index, or {@code null} for "from beginning"
     * @param end the 0-based exclusive end index, or {@code null} for "to end"
     * @param step the step interval, or {@code null} if not specified
     */
    record SliceSelector(Integer start, Integer end, Integer step) {}

    /**
     * A typed filter condition extracted from a JSONPath filter expression. Leaf conditions
     * reference a field via its JSONPath (e.g., {@code $.age}) and a comparison value. Compound
     * conditions combine leaf conditions using logical operators ({@code &&}, {@code ||},
     * {@code !}).
     */
    sealed interface FilterCondition {}

    record EqualStr(String fieldJsonPath, String value) implements FilterCondition {}

    record EqualNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record EqualBool(String fieldJsonPath, boolean value) implements FilterCondition {}

    record GreaterThanNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record LessThanNum(String fieldJsonPath, BigDecimal value) implements FilterCondition {}

    record Exists(String fieldJsonPath) implements FilterCondition {}

    record MatchRegex(String fieldJsonPath, String pattern) implements FilterCondition {}

    /** Logical AND of two filter conditions ({@code &&}). */
    record AndFilter(FilterCondition left, FilterCondition right) implements FilterCondition {}

    /** Logical OR of two filter conditions ({@code ||}). */
    record OrFilter(FilterCondition left, FilterCondition right) implements FilterCondition {}

    /** Logical negation of a filter condition ({@code !}). */
    record NotFilter(FilterCondition condition) implements FilterCondition {}

    /**
     * Analyzes a JSONPath expression and returns its structured components.
     *
     * @param jsonPath the JSONPath expression to analyze (e.g., {@code $.people[*]})
     * @return the parsed components
     * @throws IllegalArgumentException if the expression contains an unrecognized filter type
     */
    static ParsedJsonPath analyze(String jsonPath) {
        // Pre-process three-part slices ([s:e:step]) to two-part ([s:e]), capturing the steps.
        var sliceStepResult = preprocessSliceSteps(jsonPath);
        var preprocessed = preprocessUnsupportedOperators(sliceStepResult.rewrittenPath);
        var lexer = new JsonPathLexer(CharStreams.fromString(preprocessed));
        var tokens = new CommonTokenStream(lexer);
        var parser = new JsonPathParser(tokens);
        var tree = parser.path();

        var visitor = new PathAnalysisVisitor();
        visitor.visit(tree);

        // Merge extracted steps into the visitor's SliceSelectors.
        var slices = mergeSliceSteps(visitor.slices, sliceStepResult.steps);

        return new ParsedJsonPath(
                visitor.basePath.toString(),
                List.copyOf(visitor.filters),
                visitor.hasDeepScan,
                List.copyOf(slices),
                List.copyOf(visitor.unions));
    }

    /**
     * Rewrites filter comparisons using operators not supported by the JSurfer ANTLR grammar
     * ({@code !=}, {@code >=}, {@code <=}) into negated equivalents that the grammar can parse.
     *
     * <p>Each unsupported operator is replaced with a negated supported operator, and the
     * enclosing comparison is wrapped in {@code !(...)}: {@code !=} becomes {@code !(... == ...)},
     * {@code >=} becomes {@code !(... < ...)}, {@code <=} becomes {@code !(... > ...)}.
     *
     * <p>The approach finds each unsupported operator, then scans backwards for the {@code @} that
     * starts the relative path and forwards for the value end (delimited by {@code )}, {@code &},
     * or {@code |}).
     */
    private static String preprocessUnsupportedOperators(String jsonPath) {
        var matcher = UNSUPPORTED_OP.matcher(jsonPath);
        if (!matcher.find()) {
            return jsonPath;
        }

        var sb = new StringBuilder();
        var lastEnd = 0;
        matcher.reset();
        while (matcher.find()) {
            var op = matcher.group(1);
            var opStart = matcher.start();
            var opEnd = matcher.end();

            // Scan backwards from the operator to find the '@' that starts the path
            var pathStart = opStart - 1;
            while (pathStart > 0
                    && jsonPath.charAt(pathStart - 1) != '('
                    && jsonPath.charAt(pathStart - 1) != '&'
                    && jsonPath.charAt(pathStart - 1) != '|') {
                pathStart--;
            }
            // Trim leading whitespace
            while (pathStart < opStart && Character.isWhitespace(jsonPath.charAt(pathStart))) {
                pathStart++;
            }

            // Scan forwards from after the operator to find the value end
            var valueEnd = opEnd;
            while (valueEnd < jsonPath.length()
                    && jsonPath.charAt(valueEnd) != ')'
                    && jsonPath.charAt(valueEnd) != '&'
                    && jsonPath.charAt(valueEnd) != '|') {
                valueEnd++;
            }
            // Trim trailing whitespace
            var trimmedValueEnd = valueEnd;
            while (trimmedValueEnd > opEnd && Character.isWhitespace(jsonPath.charAt(trimmedValueEnd - 1))) {
                trimmedValueEnd--;
            }

            var path = jsonPath.substring(pathStart, opStart).stripTrailing();
            var value = jsonPath.substring(opEnd, trimmedValueEnd).stripLeading();
            var replacementOp = OP_REPLACEMENTS.get(op);

            sb.append(jsonPath, lastEnd, pathStart);
            sb.append("!(")
                    .append(path)
                    .append(' ')
                    .append(replacementOp)
                    .append(' ')
                    .append(value)
                    .append(')');
            lastEnd = valueEnd;
        }
        sb.append(jsonPath, lastEnd, jsonPath.length());
        return sb.toString();
    }

    /**
     * Result of pre-processing three-part slice expressions.
     *
     * @param rewrittenPath the JSONPath with three-part slices rewritten to two-part
     * @param steps the extracted step values, in order of appearance
     */
    private record SliceStepResult(String rewrittenPath, Queue<Integer> steps) {}

    /**
     * Rewrites three-part slice expressions ({@code [start:end:step]}) to two-part slices
     * ({@code [start:end]}) that the JSurfer ANTLR grammar can parse. The extracted step values
     * are returned in encounter order so they can be merged into the visitor's {@link SliceSelector}s
     * after parsing.
     */
    private static SliceStepResult preprocessSliceSteps(String jsonPath) {
        var matcher = THREE_PART_SLICE.matcher(jsonPath);
        if (!matcher.find()) {
            return new SliceStepResult(jsonPath, new LinkedList<>());
        }

        var sb = new StringBuilder();
        var steps = new LinkedList<Integer>();
        var lastEnd = 0;
        matcher.reset();
        while (matcher.find()) {
            var start = matcher.group(1);
            var end = matcher.group(2);
            var step = Integer.parseInt(matcher.group(3));

            sb.append(jsonPath, lastEnd, matcher.start());
            sb.append('[').append(start).append(':').append(end).append(']');
            steps.add(step);
            lastEnd = matcher.end();
        }
        sb.append(jsonPath, lastEnd, jsonPath.length());
        return new SliceStepResult(sb.toString(), steps);
    }

    /**
     * Merges extracted step values into the visitor's {@link SliceSelector} records. Each slice
     * produced by the visitor that corresponds to a three-part slice in the original input receives
     * the next step value from the queue.
     */
    private static List<SliceSelector> mergeSliceSteps(List<SliceSelector> visitorSlices, Queue<Integer> steps) {
        if (steps.isEmpty()) {
            return visitorSlices;
        }
        var merged = new ArrayList<SliceSelector>(visitorSlices.size());
        for (var slice : visitorSlices) {
            if (!steps.isEmpty()) {
                merged.add(new SliceSelector(slice.start(), slice.end(), steps.poll()));
            } else {
                merged.add(slice);
            }
        }
        return merged;
    }

    /**
     * ANTLR visitor that walks the JSONPath parse tree to extract the base path, filter conditions,
     * and deep scan flag. The default {@link JsonPathBaseVisitor#visitChildren} implementation
     * delegates to child-specific visit methods, so overriding the leaf visit methods is sufficient.
     */
    private static class PathAnalysisVisitor extends JsonPathBaseVisitor<Void> {

        private final StringBuilder basePath = new StringBuilder("$");
        private final List<FilterCondition> filters = new ArrayList<>();
        private final List<SliceSelector> slices = new ArrayList<>();
        private final List<UnionSelector> unions = new ArrayList<>();
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
            var indices = ctx.NUM().stream()
                    .map(num -> Integer.parseInt(num.getText()))
                    .toList();
            unions.add(new IndexUnion(indices));
            return null;
        }

        @Override
        public Void visitSlicing(JsonPathParser.SlicingContext ctx) {
            basePath.append("[*]");

            // The grammar is: '[' NUM? COLON NUM? ']'
            // Determine which NUM tokens are start vs. end by comparing their token index
            // to the COLON token index.
            var colonIndex = ctx.COLON().getSymbol().getTokenIndex();
            Integer start = null;
            Integer end = null;
            for (var num : ctx.NUM()) {
                if (num.getSymbol().getTokenIndex() < colonIndex) {
                    start = Integer.parseInt(num.getText());
                } else {
                    end = Integer.parseInt(num.getText());
                }
            }
            slices.add(new SliceSelector(start, end, null));
            return null;
        }

        @Override
        public Void visitChildrenNode(JsonPathParser.ChildrenNodeContext ctx) {
            basePath.append("[*]");
            var names = ctx.QUOTED_STRING().stream()
                    .map(qs -> {
                        var raw = qs.getText();
                        return raw.substring(1, raw.length() - 1);
                    })
                    .toList();
            unions.add(new NameUnion(names));
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
            if (ctx.AndOperator() != null) {
                return new AndFilter(
                        extractFilterCondition(ctx.filterExpr(0)), extractFilterCondition(ctx.filterExpr(1)));
            }
            if (ctx.OrOperator() != null) {
                return new OrFilter(
                        extractFilterCondition(ctx.filterExpr(0)), extractFilterCondition(ctx.filterExpr(1)));
            }
            if (ctx.NegationOperator() != null) {
                return new NotFilter(extractFilterCondition(ctx.filterExpr(0)));
            }

            throw new IllegalArgumentException("Unrecognized JSONPath filter expression type in JsonPathAnalyzer");
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
