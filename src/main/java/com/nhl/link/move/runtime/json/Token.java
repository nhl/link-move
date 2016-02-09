package com.nhl.link.move.runtime.json;

class Token {

    enum Type {
        ROOT_NODE_REF, CURRENT_NODE_REF, CHILD_ACCESS, RECURSIVE_DESCENT, WILDCARD, UNION,
        FILTER_START, FILTER_END, PREDICATE_START, PREDICATE_END, IDENTIFIER, QUOTED_IDENTIFIER,
        NUMERIC_VALUE
    }

    private Type type;
    private String literal;
    private int position;

    Token(Type type, String literal, int position) {
        this.type = type;
        this.literal = literal;
        this.position = position;
    }

    Type getType() {
        return type;
    }

    String getLiteral() {
        return literal;
    }

    int getPosition() {
        return position;
    }
}
