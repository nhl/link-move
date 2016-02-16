package com.nhl.link.move.runtime.json.query;

class Token {

    private TokenType type;
    private String literal;
    private int position;

    Token(TokenType type, String literal, int position) {
        this.type = type;
        this.literal = literal;
        this.position = position;
    }

    TokenType getType() {
        return type;
    }

    String getLiteral() {
        return literal;
    }

    int getPosition() {
        return position;
    }
}
