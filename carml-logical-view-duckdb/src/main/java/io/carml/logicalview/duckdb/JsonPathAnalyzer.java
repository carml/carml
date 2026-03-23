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
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
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
 *
 * <p>RFC 9535 filter function extensions ({@code length()}, {@code count()}, {@code match()},
 * {@code search()}, {@code value()}) are also not supported by the JSurfer grammar. A separate
 * pre-processing step runs <b>before</b> the operator rewriter: it replaces function calls with
 * placeholder expressions ({@code @.__fn0 == true}) that the grammar can parse, stores the real
 * function conditions, and swaps them back into the parsed tree after ANTLR processing.
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

    /** Comparison operator for function result comparisons (RFC 9535 §2.4). */
    enum CompOp {
        EQ,
        NEQ,
        GT,
        LT,
        GTE,
        LTE
    }

    /**
     * Compares the length of a JSON value (array length, string length, or object member count)
     * against a numeric value. Corresponds to the RFC 9535 {@code length()} and {@code count()}
     * function extensions.
     *
     * @param fieldJsonPath the JSONPath to the field whose length to measure
     * @param op the comparison operator
     * @param value the numeric value to compare against
     */
    record LengthCompare(String fieldJsonPath, CompOp op, BigDecimal value) implements FilterCondition {}

    /**
     * Tests if the entire string value matches a regular expression. Corresponds to the RFC 9535
     * {@code match()} function extension.
     *
     * @param fieldJsonPath the JSONPath to the field to test
     * @param pattern the regular expression pattern (full match required)
     */
    record FullMatch(String fieldJsonPath, String pattern) implements FilterCondition {}

    /**
     * Tests if the string value contains a substring matching a regular expression. Corresponds to
     * the RFC 9535 {@code search()} function extension.
     *
     * @param fieldJsonPath the JSONPath to the field to test
     * @param pattern the regular expression pattern (partial match)
     */
    record PartialMatch(String fieldJsonPath, String pattern) implements FilterCondition {}

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
     * @throws IllegalArgumentException if the expression has invalid syntax or contains an
     *     unrecognized filter type
     */
    static ParsedJsonPath analyze(String jsonPath) {
        // 1. Pre-process function calls (before operator rewriting, so functions handle all ops).
        var functionResult = preprocessFunctions(jsonPath);
        // 2. Pre-process three-part slices ([s:e:step]) to two-part ([s:e]), capturing the steps.
        var sliceStepResult = preprocessSliceSteps(functionResult.rewrittenPath);
        // 3. Rewrite unsupported operators (!=, >=, <=) to negated equivalents.
        var preprocessed = preprocessUnsupportedOperators(sliceStepResult.rewrittenPath);

        var lexer = new JsonPathLexer(CharStreams.fromString(preprocessed));
        var tokens = new CommonTokenStream(lexer);
        var parser = new JsonPathParser(tokens);

        // Collect syntax errors instead of printing them to stderr. Both the lexer and parser
        // have default error listeners that write to stderr, which pollutes test output.
        var syntaxErrors = new ArrayList<String>();
        var errorListener = new BaseErrorListener() {
            @Override
            public void syntaxError(
                    Recognizer<?, ?> recognizer,
                    Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    String msg,
                    RecognitionException ex) {
                syntaxErrors.add("at position %d: %s".formatted(charPositionInLine, msg));
            }
        };
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        var tree = parser.path();

        if (!syntaxErrors.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid JSONPath expression '%s': %s".formatted(jsonPath, String.join("; ", syntaxErrors)));
        }

        var visitor = new PathAnalysisVisitor();
        visitor.visit(tree);

        // 4. Replace function placeholders with real function conditions.
        var filters = replaceFunctionPlaceholders(visitor.filters, functionResult.conditions);
        // 5. Merge extracted steps into the visitor's SliceSelectors.
        var slices = mergeSliceSteps(visitor.slices, sliceStepResult.steps);

        return new ParsedJsonPath(
                visitor.basePath.toString(),
                List.copyOf(filters),
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
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
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

    /** Placeholder field name used in the rewritten JSONPath to stand in for a function call. */
    private static final String FN_PLACEHOLDER_PREFIX = "__fn";

    private static final String FN_MATCH = "match";
    private static final String FN_SEARCH = "search";
    private static final String FN_VALUE = "value";
    private static final String FN_LENGTH = "length";
    private static final String FN_COUNT = "count";

    private static final List<String> FUNCTION_NAMES = List.of(FN_LENGTH, FN_COUNT, FN_MATCH, FN_SEARCH, FN_VALUE);

    /**
     * Result of pre-processing filter function extensions.
     *
     * @param rewrittenPath the JSONPath with function calls replaced by placeholders
     * @param conditions the extracted function conditions, keyed by placeholder index
     */
    private record FunctionPreprocessResult(String rewrittenPath, List<FilterCondition> conditions) {}

    /**
     * Scans the JSONPath for RFC 9535 filter function extensions ({@code length()}, {@code count()},
     * {@code match()}, {@code search()}, {@code value()}) and replaces them with placeholder
     * expressions that JSurfer can parse.
     *
     * <p>Each function call is replaced by {@code @.__fnN == true} (where N is a 0-based counter),
     * and the corresponding {@link FilterCondition} is stored for later substitution. The
     * {@code value()} function is special-cased: it is rewritten to the bare path argument with no
     * placeholder needed.
     *
     * <p>This method runs <b>before</b> the operator pre-processor so that comparison operators
     * inside function comparisons (e.g., {@code length(@.tags) >= 5}) are handled here rather than
     * by the operator rewriter.
     */
    private static FunctionPreprocessResult preprocessFunctions(String jsonPath) {
        var conditions = new ArrayList<FilterCondition>();
        var sb = new StringBuilder();
        var idx = 0;
        var len = jsonPath.length();
        var rewritten = false;

        while (idx < len) {
            var fnMatch = matchFunctionCall(jsonPath, idx);
            if (fnMatch != null) {
                rewritten = true;
                var condition = buildFunctionCondition(fnMatch);
                if (condition != null) {
                    // Function that produces a placeholder
                    var placeholderIndex = conditions.size();
                    conditions.add(condition);
                    sb.append("@.%s%d == true".formatted(FN_PLACEHOLDER_PREFIX, placeholderIndex));
                } else {
                    // value() function: rewrite to bare path, preserving any comparison after it
                    sb.append(fnMatch.args.get(0));
                    if (fnMatch.compOp != null) {
                        sb.append(' ').append(fnMatch.compOp).append(' ').append(fnMatch.compValue);
                    }
                }
                idx = fnMatch.fullExpressionEnd;
            } else {
                sb.append(jsonPath.charAt(idx));
                idx++;
            }
        }

        if (!rewritten) {
            return new FunctionPreprocessResult(jsonPath, List.of());
        }
        return new FunctionPreprocessResult(sb.toString(), conditions);
    }

    /**
     * Parsed function call with its position and arguments.
     *
     * @param functionName the function name (e.g., "length", "match")
     * @param args the function arguments as raw strings
     * @param callEnd the character index just past the closing parenthesis of the function call
     * @param compOp the comparison operator following the function call, or {@code null} for boolean
     *     functions
     * @param compValue the comparison value following the operator, or {@code null} for boolean
     *     functions
     * @param fullExpressionEnd the character index just past the full expression including operator
     *     and value
     */
    private record FunctionCallMatch(
            String functionName,
            List<String> args,
            int callEnd,
            String compOp,
            String compValue,
            int fullExpressionEnd) {}

    /**
     * Attempts to match a function call at the given position. Returns {@code null} if no function
     * call is found. Uses character scanning with parenthesis depth tracking and string literal
     * awareness.
     */
    private static FunctionCallMatch matchFunctionCall(String jsonPath, int pos) {
        // Check for known function names
        var funcName = matchFunctionName(jsonPath, pos);
        if (funcName == null) {
            return null;
        }

        var nameEnd = pos + funcName.length();
        if (nameEnd >= jsonPath.length() || jsonPath.charAt(nameEnd) != '(') {
            return null;
        }

        // Parse arguments by scanning for the matching closing parenthesis
        var argsStart = nameEnd + 1;
        var args = parseFunctionArgs(jsonPath, argsStart);
        if (args == null) {
            return null;
        }

        var callEnd = args.endIndex;

        // For boolean functions (match, search), there's no comparison operator
        if (FN_MATCH.equals(funcName) || FN_SEARCH.equals(funcName)) {
            return new FunctionCallMatch(funcName, args.arguments, callEnd, null, null, callEnd);
        }

        // For value/length/count, scan for comparison operator and value
        return scanComparisonAfterCall(jsonPath, funcName, args.arguments, callEnd);
    }

    /**
     * Checks if one of the known function names starts at the given position. Only matches
     * when the character before the function name is a boundary character (filter expression
     * start, logical operator, or negation).
     */
    private static String matchFunctionName(String jsonPath, int pos) {
        // Functions only appear inside filter expressions. The character before the function
        // name must be '(', '!', '&', '|', or whitespace preceded by one of those.
        if (pos > 0) {
            var preceding = findPrecedingNonWhitespace(jsonPath, pos);
            if (preceding != '(' && preceding != '!' && preceding != '&' && preceding != '|') {
                return null;
            }
        }

        for (var name : FUNCTION_NAMES) {
            if (jsonPath.startsWith(name, pos)) {
                var end = pos + name.length();
                if (end < jsonPath.length() && jsonPath.charAt(end) == '(') {
                    return name;
                }
            }
        }
        return null;
    }

    /** Finds the nearest non-whitespace character before the given position. */
    private static char findPrecedingNonWhitespace(String jsonPath, int pos) {
        var position = pos - 1;
        while (position >= 0 && Character.isWhitespace(jsonPath.charAt(position))) {
            position--;
        }
        return position >= 0 ? jsonPath.charAt(position) : '\0';
    }

    /**
     * Parsed function arguments.
     *
     * @param arguments the list of argument strings (trimmed)
     * @param endIndex the character index just past the closing parenthesis
     */
    private record ParsedArgs(List<String> arguments, int endIndex) {}

    /**
     * Parses comma-separated function arguments starting at the given position (just past the
     * opening parenthesis). Handles nested parentheses and quoted string literals.
     */
    private static ParsedArgs parseFunctionArgs(String jsonPath, int start) {
        var state = new ArgParseState(jsonPath);
        state.pos = start;

        while (state.pos < jsonPath.length() && state.depth > 0) {
            state.processChar();
        }

        if (state.depth != 0) {
            return null; // Unbalanced parentheses
        }

        return new ParsedArgs(state.args, state.pos);
    }

    /** Mutable state for argument parsing, extracted to reduce cognitive complexity. */
    private static class ArgParseState {
        private final String jsonPath;
        private final List<String> args = new ArrayList<>();
        private final StringBuilder current = new StringBuilder();
        private int depth = 1;
        private int pos;

        ArgParseState(String jsonPath) {
            this.jsonPath = jsonPath;
        }

        void processChar() {
            var ch = jsonPath.charAt(pos);
            if (ch == '\'' || ch == '"') {
                var closeQuote = scanPastStringLiteral(jsonPath, pos);
                current.append(jsonPath, pos, closeQuote);
                pos = closeQuote;
            } else if (ch == '(') {
                depth++;
                current.append(ch);
                pos++;
            } else if (ch == ')') {
                handleCloseParen();
            } else if (ch == ',' && depth == 1) {
                args.add(current.toString().strip());
                current.setLength(0);
                pos++;
            } else {
                current.append(ch);
                pos++;
            }
        }

        private void handleCloseParen() {
            depth--;
            if (depth > 0) {
                current.append(')');
            } else {
                args.add(current.toString().strip());
            }
            pos++;
        }
    }

    /**
     * Scans past a string literal (single or double quoted) starting at position {@code start}.
     * Returns the index just past the closing quote.
     */
    private static int scanPastStringLiteral(String jsonPath, int start) {
        var quote = jsonPath.charAt(start);
        var idx = start + 1;
        while (idx < jsonPath.length()) {
            if (jsonPath.charAt(idx) == '\\') {
                idx += 2; // Skip escaped character
            } else if (jsonPath.charAt(idx) == quote) {
                return idx + 1;
            } else {
                idx++;
            }
        }
        return idx;
    }

    /**
     * Scans for a comparison operator and value after a function call. For {@code value()}, the
     * comparison is kept as part of the full expression so the rewrite can inline the bare path
     * with the operator. For {@code length()} and {@code count()}, the comparison is extracted
     * into a {@link FunctionCallMatch}.
     */
    private static FunctionCallMatch scanComparisonAfterCall(
            String jsonPath, String funcName, List<String> args, int callEnd) {
        var idx = callEnd;
        var len = jsonPath.length();

        // Skip whitespace
        while (idx < len && Character.isWhitespace(jsonPath.charAt(idx))) {
            idx++;
        }

        // Scan for a comparison operator
        var opResult = scanComparisonOp(jsonPath, idx);
        if (opResult == null) {
            // No comparison operator — for value() this means the expression is just `value(@.path)`,
            // which might be used in an existence check context. Treat fullExpressionEnd as callEnd.
            return FN_VALUE.equals(funcName)
                    ? new FunctionCallMatch(funcName, args, callEnd, null, null, callEnd)
                    : null;
        }

        // Skip whitespace after operator
        var valueStart = opResult.opEnd;
        while (valueStart < len && Character.isWhitespace(jsonPath.charAt(valueStart))) {
            valueStart++;
        }

        // Scan for the comparison value (delimited by ')', '&', '|', or end of string)
        var valueEnd = scanValueEnd(jsonPath, valueStart);

        var compValue = jsonPath.substring(valueStart, valueEnd).strip();
        return new FunctionCallMatch(funcName, args, callEnd, opResult.op, compValue, valueEnd);
    }

    /**
     * Result of scanning a comparison operator.
     *
     * @param op the operator string
     * @param opEnd the index just past the operator
     */
    private record OpScanResult(String op, int opEnd) {}

    /** Scans for a comparison operator at the given position. */
    private static OpScanResult scanComparisonOp(String jsonPath, int pos) {
        if (pos >= jsonPath.length()) {
            return null;
        }
        // Two-character operators first
        if (pos + 1 < jsonPath.length()) {
            var twoChar = jsonPath.substring(pos, pos + 2);
            if ("==".equals(twoChar) || "!=".equals(twoChar) || ">=".equals(twoChar) || "<=".equals(twoChar)) {
                return new OpScanResult(twoChar, pos + 2);
            }
        }
        // Single-character operators
        var ch = jsonPath.charAt(pos);
        if (ch == '>' || ch == '<') {
            return new OpScanResult(String.valueOf(ch), pos + 1);
        }
        return null;
    }

    /** Scans forward to find the end of a comparison value, delimited by ')', '&', or '|'. */
    private static int scanValueEnd(String jsonPath, int start) {
        var idx = start;
        var len = jsonPath.length();
        while (idx < len) {
            var ch = jsonPath.charAt(idx);
            if (ch == ')' || ch == '&' || ch == '|') {
                break;
            }
            if (ch == '\'' || ch == '"') {
                idx = scanPastStringLiteral(jsonPath, idx);
            } else {
                idx++;
            }
        }
        // Trim trailing whitespace
        var end = idx;
        while (end > start && Character.isWhitespace(jsonPath.charAt(end - 1))) {
            end--;
        }
        return end;
    }

    /**
     * Builds the appropriate {@link FilterCondition} for a parsed function call, or returns
     * {@code null} for {@code value()} (which is handled by inline rewriting instead).
     */
    private static FilterCondition buildFunctionCondition(FunctionCallMatch match) {
        var funcName = match.functionName;
        if (FN_VALUE.equals(funcName)) {
            return null; // Handled by inline rewriting
        }
        if (FN_MATCH.equals(funcName) || FN_SEARCH.equals(funcName)) {
            return buildRegexCondition(match);
        }
        if (FN_LENGTH.equals(funcName) || FN_COUNT.equals(funcName)) {
            return buildLengthCondition(match);
        }
        return null;
    }

    /** Builds a {@link FullMatch} or {@link PartialMatch} from a match/search function call. */
    private static FilterCondition buildRegexCondition(FunctionCallMatch match) {
        if (match.args.size() < 2) {
            throw new IllegalArgumentException(
                    "Function %s() requires 2 arguments, got %d".formatted(match.functionName, match.args.size()));
        }
        var pathArg = normalizeFilterPath(match.args.get(0));
        var patternArg = unquote(match.args.get(1));

        if (FN_MATCH.equals(match.functionName)) {
            return new FullMatch(pathArg, patternArg);
        }
        return new PartialMatch(pathArg, patternArg);
    }

    /** Builds a {@link LengthCompare} from a length/count function call. */
    private static FilterCondition buildLengthCondition(FunctionCallMatch match) {
        if (match.args.isEmpty()) {
            throw new IllegalArgumentException(
                    "Function %s() requires at least 1 argument".formatted(match.functionName));
        }
        var pathArg = normalizeFilterPath(match.args.get(0));

        // For count(), strip trailing [*] from the path
        if (FN_COUNT.equals(match.functionName) && pathArg.endsWith("[*]")) {
            pathArg = pathArg.substring(0, pathArg.length() - 3);
        }

        var compOp = parseCompOp(match.compOp);
        var compValue = new BigDecimal(match.compValue);

        return new LengthCompare(pathArg, compOp, compValue);
    }

    /**
     * Normalizes a filter-relative path argument (e.g., {@code @.name}) to a root-relative path
     * (e.g., {@code $.name}).
     */
    private static String normalizeFilterPath(String pathArg) {
        var stripped = pathArg.strip();
        if (stripped.startsWith("@")) {
            return "$" + stripped.substring(1);
        }
        return stripped;
    }

    /** Removes surrounding single or double quotes from a string argument. */
    private static String unquote(String arg) {
        var stripped = arg.strip();
        if (stripped.length() >= 2) {
            var first = stripped.charAt(0);
            var last = stripped.charAt(stripped.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return stripped.substring(1, stripped.length() - 1);
            }
        }
        return stripped;
    }

    /** Parses a comparison operator string into a {@link CompOp} enum value. */
    private static CompOp parseCompOp(String op) {
        if ("==".equals(op)) {
            return CompOp.EQ;
        } else if ("!=".equals(op)) {
            return CompOp.NEQ;
        } else if (">".equals(op)) {
            return CompOp.GT;
        } else if ("<".equals(op)) {
            return CompOp.LT;
        } else if (">=".equals(op)) {
            return CompOp.GTE;
        } else if ("<=".equals(op)) {
            return CompOp.LTE;
        }
        throw new IllegalArgumentException("Unknown comparison operator: %s".formatted(op));
    }

    /**
     * Walks the filter condition tree and replaces placeholder {@link EqualBool} conditions
     * (matching {@code $.__fnN == true}) with the corresponding real function conditions from the
     * pre-processing step.
     */
    private static List<FilterCondition> replaceFunctionPlaceholders(
            List<FilterCondition> filters, List<FilterCondition> functionConditions) {
        if (functionConditions.isEmpty()) {
            return filters;
        }
        return filters.stream()
                .map(f -> replacePlaceholder(f, functionConditions))
                .toList();
    }

    /** Recursively replaces function placeholders in a single filter condition. */
    private static FilterCondition replacePlaceholder(
            FilterCondition condition, List<FilterCondition> functionConditions) {
        if (condition instanceof EqualBool eb) {
            var idx = extractPlaceholderIndex(eb.fieldJsonPath());
            if (idx >= 0 && eb.value() && idx < functionConditions.size()) {
                return functionConditions.get(idx);
            }
        } else if (condition instanceof AndFilter af) {
            return new AndFilter(
                    replacePlaceholder(af.left(), functionConditions),
                    replacePlaceholder(af.right(), functionConditions));
        } else if (condition instanceof OrFilter of) {
            return new OrFilter(
                    replacePlaceholder(of.left(), functionConditions),
                    replacePlaceholder(of.right(), functionConditions));
        } else if (condition instanceof NotFilter nf) {
            return new NotFilter(replacePlaceholder(nf.condition(), functionConditions));
        }
        return condition;
    }

    /**
     * Extracts the placeholder index from a field path like {@code $.__fn0}. Returns -1 if the
     * path does not match the placeholder pattern.
     */
    private static int extractPlaceholderIndex(String fieldJsonPath) {
        var prefix = "$." + FN_PLACEHOLDER_PREFIX;
        if (fieldJsonPath.startsWith(prefix)) {
            try {
                return Integer.parseInt(fieldJsonPath.substring(prefix.length()));
            } catch (NumberFormatException ignored) {
                return -1;
            }
        }
        return -1;
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

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
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
