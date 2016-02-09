package com.nhl.link.move.runtime.json;

import com.nhl.link.move.runtime.json.query.AllProperties;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.Token.Type;
import com.nhl.link.move.runtime.json.query.NamedProperty;
import com.nhl.link.move.runtime.json.query.RecursiveDescent;
import com.nhl.link.move.runtime.json.query.RootNode;

public class QueryCompiler {

    public JsonQuery compile(String queryStr) {

        Scanner scanner = new Scanner(queryStr);
        if (!scanner.hasNext()) {
            throw new RuntimeException("Empty query");
        }
        return buildRootQuery(scanner);
    }

    private JsonQuery buildRootQuery(Scanner scanner) {

        Token token = scanner.nextToken();
        if (token.getType() != Type.ROOT_NODE_REF) {
            throw new ParseException(token, Type.ROOT_NODE_REF);
        }
        return new RootNode(buildQuery(scanner));
    }

    private JsonQuery buildQuery(Scanner scanner) {

        if (!scanner.hasNext()) {
            return null;
        }

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case CHILD_ACCESS: {
                return buildChildOrFilter(scanner);
            }
            case RECURSIVE_DESCENT: {
                return new RecursiveDescent(buildChildOrFilter(scanner));
            }
            case FILTER_START: {
                return buildFilter(scanner);
            }
            default: {
                throw new ParseException(token, Type.CHILD_ACCESS, Type.RECURSIVE_DESCENT);
            }
        }
    }

    private JsonQuery buildChildOrFilter(Scanner scanner) {

        assertHasTokens(scanner);

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case NUMERIC_VALUE:
            case IDENTIFIER: {
                return new NamedProperty(buildQueryOrFilter(scanner), token.getLiteral());
            }
            case QUOTED_IDENTIFIER: {
                throw new ParseException("Quoted identifier not allowed here", token.getPosition());
            }
            case FILTER_START: {
                return buildFilter(scanner);
            }
            case WILDCARD: {
                return new AllProperties(buildQuery(scanner));
            }
            default: {
                throw new ParseException(token, Type.IDENTIFIER, Type.FILTER_START, Type.WILDCARD);
            }
        }
    }

    private JsonQuery buildQueryOrFilter(Scanner scanner) {

        if (!scanner.hasNext()) {
            return null;
        }

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case FILTER_START: {
                return buildFilter(scanner);
            }
            case CHILD_ACCESS: {
                return buildChildOrFilter(scanner);
            }
            case RECURSIVE_DESCENT: {
                return new RecursiveDescent(buildChildOrFilter(scanner));
            }
            default: {
                throw new ParseException(token, Type.FILTER_START, Type.CHILD_ACCESS, Type.RECURSIVE_DESCENT);
            }
        }
    }

    private JsonQuery buildFilter(Scanner scanner) {

        assertHasTokens(scanner);

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case NUMERIC_VALUE:
            case QUOTED_IDENTIFIER: {
                assertNextToken(scanner, Type.FILTER_END);
                return new NamedProperty(buildQuery(scanner), token.getLiteral());
            }
            case IDENTIFIER: {
                throw new ParseException(
                        "Unquoted identifier not allowed here: " + token.getLiteral(), token.getPosition());
            }
            case WILDCARD: {
                assertNextToken(scanner, Type.FILTER_END);
                return new AllProperties(buildQuery(scanner));
            }
            case PREDICATE_START: {
                throw new RuntimeException("Predicates not implemented yet");
            }
            default: {
                throw new ParseException(token, Type.IDENTIFIER, Type.WILDCARD, Type.PREDICATE_START);
            }
        }
    }

    private static void assertHasTokens(Scanner scanner) {
        if (!scanner.hasNext()) {
            throw new RuntimeException("Premature end of query string");
        }
    }

    private static void assertNextToken(Scanner scanner, Type tokenType) {
        assertHasTokens(scanner);
        Token token = scanner.nextToken();
        if (tokenType != token.getType()) {
            throw new ParseException(token, tokenType);
        }
    }
}
