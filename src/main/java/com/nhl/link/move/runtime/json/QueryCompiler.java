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

        Token token = scanner.nextToken();
        if (token.getType() != Type.ROOT_NODE_REF) {
            throw new RuntimeException("Query must begin with root designator '$'");
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
            default: {
                throw new RuntimeException(
                        "Unexpected token '" + token.getLiteral() + "' at position: " + token.getPosition());
            }
        }
    }

    private JsonQuery buildChildOrFilter(Scanner scanner) {

        assertHasTokens(scanner);

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case IDENTIFIER: {
                return new NamedProperty(buildQueryOrFilter(scanner), token.getLiteral());
            }
            case FILTER_START: {
                return buildFilter(scanner);
            }
            case WILDCARD: {
                return new AllProperties(buildQuery(scanner));
            }
            default: {
                throw new RuntimeException(
                        "Unexpected token '" + token.getLiteral() + "' at position: " + token.getPosition());
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
                throw new RuntimeException(
                        "Unexpected token '" + token.getLiteral() + "' at position: " + token.getPosition());
            }
        }
    }

    private JsonQuery buildFilter(Scanner scanner) {

        assertHasTokens(scanner);

        Token token = scanner.nextToken();
        switch (token.getType()) {
            case IDENTIFIER: {
                assertNextToken(scanner, Type.FILTER_END);
                return new NamedProperty(buildQuery(scanner), token.getLiteral());
            }
            case WILDCARD: {
                assertNextToken(scanner, Type.FILTER_END);
                return new AllProperties(buildQuery(scanner));
            }
            case PREDICATE_START: {
                throw new RuntimeException("Predicates not implemented yet");
            }
            default: {
                throw new RuntimeException(
                        "Unexpected token '" + token.getLiteral() + "' at position: " + token.getPosition());
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
            throw new RuntimeException("Expected token of type " + tokenType.name() +
                    ", but received: " + token.getLiteral() + " at position " + token.getPosition());
        }
    }
}
