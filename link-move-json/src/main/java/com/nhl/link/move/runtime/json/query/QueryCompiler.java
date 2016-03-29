package com.nhl.link.move.runtime.json.query;

import com.nhl.link.move.runtime.json.query.script.IOpService;
import com.nhl.link.move.runtime.json.query.script.OpService;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

public class QueryCompiler {

    private static final String LOGICAL_AND = "&&", LOGICAL_OR = "||";

    public JsonQuery compile(String queryStr) {
        return new Parser(queryStr).parse();
    }

    private static class Parser {

        private final String queryStr;
        private IOpService opService;
        private Scanner scanner;
        private Context context;

        Parser(String queryStr) {

            this.queryStr = queryStr;

            opService = new OpService();
            scanner = new Scanner(queryStr);
            if (!scanner.hasNext()) {
                throw new RuntimeException("Empty query");
            }
            context = new Context();
        }

        JsonQuery parse() {
            JsonQuery query = buildQuery();
            if (context.insideScript()) {
                throw new RuntimeException("Premature end of query string: not all filters and scripts were closed");
            }
            return query;
        }

        private JsonQuery buildQuery() {

            assertHasTokens();

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case ROOT_NODE_REF: {
                    return new RootNode(buildSegment());
                }
                case CURRENT_NODE_REF: {
                    return new CurrentNode(buildSegment());
                }
                default: {
                    return new NamedProperty(null, queryStr);
                }
            }
        }

        /**
         * Primary function for building queries
         */
        private JsonQuery buildSegment() {

            if (!scanner.hasNext()) {
                return null;
            }

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case CHILD_ACCESS: {
                    return buildChildOrFilter();
                }
                case RECURSIVE_DESCENT: {
                    return new RecursiveDescent(buildChildOrFilter());
                }
                case FILTER_START: {
                    return buildFilter();
                }
                case FILTER_END: {
                    if (context.insideFilter()) {
                        context.exitFilter();
                        // continue building query
                        return buildSegment();
                    }
                    // fall through
                }
                case SCRIPT_END:
                case IDENTIFIER: {
                    if (context.insideScript()) {
                        // put operator name on stack:
                        // script manages nestedness on its own
                        scanner.returnToken(token);
                        return null;
                    }
                    // fall through
                }
                default: {
                    List<TokenType> types = new ArrayList<>();
                    types.addAll(Arrays.asList(TokenType.CHILD_ACCESS, TokenType.RECURSIVE_DESCENT, TokenType.FILTER_START));
                    if (context.insideFilter()) {
                        types.add(TokenType.FILTER_END);
                    }
                    if (context.insideScript()) {
                        types.add(TokenType.SCRIPT_END);
                        types.add(TokenType.IDENTIFIER);
                    }
                    throw new ParseException(token, types.toArray(new TokenType[types.size()]));
                }
            }
        }

        private JsonQuery buildChildOrFilter() {

            assertHasTokens();

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case NUMERIC_VALUE:
                case IDENTIFIER: {
                    return new NamedProperty(buildSegment(), token.getLiteral());
                }
                case FILTER_START: {
                    return buildFilter();
                }
                case WILDCARD: {
                    return new AllProperties(buildSegment());
                }
                // neat message instead of Unexpected token
                case QUOTED_IDENTIFIER: {
                    throw new ParseException("Quoted identifier not allowed here", token.getPosition());
                }
                // generic Unexpected... errors
                default: {
                    throw new ParseException(token, TokenType.NUMERIC_VALUE, TokenType.IDENTIFIER, TokenType.FILTER_START, TokenType.WILDCARD);
                }
            }
        }

        private JsonQuery buildFilter() {

            assertHasTokens();

            context.enterFilter();

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case NUMERIC_VALUE:
                case QUOTED_IDENTIFIER: {
                    assertNextToken(TokenType.FILTER_END);
                    return new NamedProperty(buildSegment(), token.getLiteral());
                }
                case WILDCARD: {
                    assertNextToken(TokenType.FILTER_END);
                    return new AllProperties(buildSegment());
                }
                case PREDICATE_START: {
                    return buildPredicate();
                }
                case SCRIPT_START: {
                    return new DynamicNamedProperty(buildScript(), buildSegment());
                }
                case IDENTIFIER: {
                    throw new ParseException(
                            "Unquoted identifier not allowed here: " + token.getLiteral(), token.getPosition());
                }
                default: {
                    throw new ParseException(token, TokenType.NUMERIC_VALUE, TokenType.QUOTED_IDENTIFIER,
                            TokenType.WILDCARD, TokenType.PREDICATE_START, TokenType.SCRIPT_START);
                }
            }
        }

        private JsonQuery buildPredicate() {

            assertHasTokens();

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case SCRIPT_START: {
                    return new Predicate(buildScript(), buildSegment());
                }
                default: {
                    throw new ParseException(token, TokenType.SCRIPT_START);
                }
            }
        }

        private JsonQuery buildScript() {

            context.enterScript();

            outer:
            while (scanner.hasNext()) {
                Token token = scanner.nextToken();
                switch (token.getType()) {
                    case NUMERIC_VALUE: {
                        // TODO: other numeric types
                        context.pushOnStack(ConstantValue.valueOf(Integer.valueOf(token.getLiteral())));
                        break;
                    }
                    case QUOTED_IDENTIFIER: {
                        context.pushOnStack(ConstantValue.valueOf(token.getLiteral()));
                        break;
                    }
                    case IDENTIFIER: {
                        String literal = token.getLiteral();
                        if (opService.isOp(literal)) {
                            context.pushOnStack(token);
                            continue; // do not compact stack when last element is operator
                        } else if (literal.equals(Boolean.FALSE.toString()) || literal.equals(Boolean.TRUE.toString())) {
                            context.pushOnStack(ConstantValue.valueOf(Boolean.valueOf(literal)));
                            break;
                        } else {
                            throw new ParseException("Unknown operator: " + token.getLiteral(), token.getPosition());
                        }
                    }
                    case SCRIPT_END: {
                        break outer;
                    }
                    default: {
                        scanner.returnToken(token);
                        context.pushOnStack(buildScriptSegment());
                    }
                }

                compactStack(Arrays.asList(LOGICAL_AND, LOGICAL_OR));
            }

            compactStack(Collections.singletonList(LOGICAL_OR));
            compactStack(Collections.<String>emptyList());
            JsonQuery query = (JsonQuery) context.pollFromStack();
            context.exitScript();
            return query;
        }

        private JsonQuery buildScriptSegment() {

            Token token = scanner.nextToken();
            switch (token.getType()) {
                case ROOT_NODE_REF: {
                    return new RootNode(buildSegment());
                }
                case CURRENT_NODE_REF: {
                    return new CurrentNode(buildSegment());
                }
                case SCRIPT_START: {
                    // nested script
                    return buildScript();
                }
                default: {
                    throw new ParseException(token, TokenType.ROOT_NODE_REF, TokenType.CURRENT_NODE_REF, TokenType.SCRIPT_START);
                }
            }
        }

        private void compactStack(Collection<String> ignoredOps) {

            Object rhsQueryObj = context.pollFromStack();
            if (rhsQueryObj instanceof Token) {
                Token t = (Token) rhsQueryObj;
                throw new ParseException(String.format(
                        "Operator '%s' is missing right-hand side", t.getLiteral()), t.getPosition());
            }
            JsonQuery rhsQuery = (JsonQuery) rhsQueryObj;

            Object tokenObj = context.pollFromStack();
            if (tokenObj instanceof JsonQuery) {
                throw new ParseException("Missing operator in query string");
            }
            Token operator = (Token) tokenObj;

            if (operator == null) {
                // nothing to compact
                context.pushOnStack(rhsQuery);
            } else {

                String literal = operator.getLiteral();
                if (ignoredOps.contains(literal)) {
                    compactStack(ignoredOps);
                    context.pushOnStack(operator);
                    context.pushOnStack(rhsQuery);

                } else {

                    Object lhsQueryObj = context.pollFromStack();
                    if (lhsQueryObj instanceof Token) {
                        Token t = (Token) lhsQueryObj;
                        throw new ParseException(String.format(
                                "Operator '%s' is missing left-hand side", t.getLiteral()), t.getPosition());
                    }

                    JsonQuery lhsQuery = (JsonQuery) lhsQueryObj;
                    if (lhsQuery == null) {
                        throw new ParseException("Left-hand side is missing in operator: " + operator.getLiteral(),
                                operator.getPosition());
                    }
                    context.pushOnStack(opService.buildOp(operator.getLiteral(), lhsQuery, rhsQuery));

                    compactStack(ignoredOps);
                }
            }
        }

        private void assertHasTokens() {
            if (!scanner.hasNext()) {
                throw new RuntimeException("Premature end of query string");
            }
        }

        private void assertNextToken(TokenType tokenType) {
            assertHasTokens();
            Token token = scanner.nextToken();
            if (tokenType != token.getType()) {
                throw new ParseException(token, tokenType);
            }
        }

        private static class Context {

            private int scriptLevel, filterLevel;
            private List<Deque<Object>> parseStacks = new ArrayList<>();

            void enterScript() {
                parseStacks.add(new ArrayDeque<>());
                scriptLevel++;
            }

            boolean insideScript() {
                return scriptLevel > 0;
            }

            void exitScript() {
                if (scriptLevel == 0) {
                    throw new RuntimeException("Not inside a script");
                }
                if (currentStack().size() > 0) {
                    throw new RuntimeException("Attempted to exit script with unused elements on stack");
                }
                scriptLevel--;
                parseStacks.remove(scriptLevel);
            }

            private Deque<Object> currentStack() {
                if (!insideScript()) {
                    throw new RuntimeException("Requested stack while not inside a script");
                }
                return parseStacks.get(scriptLevel - 1);
            }

            void enterFilter() {
                filterLevel++;
            }

            boolean insideFilter() {
                return filterLevel > 0;
            }

            void exitFilter() {
                if (filterLevel == 0) {
                    throw new RuntimeException("Not inside a filter");
                }
                filterLevel--;
            }

            void pushOnStack(Object element) {
                currentStack().addLast(element);
            }

            Object pollFromStack() {
                return currentStack().pollLast();
            }
        }
    }
}
