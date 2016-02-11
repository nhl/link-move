package com.nhl.link.move.runtime.json.query;

class ParseException extends RuntimeException {

    private final String message;

    ParseException(String message) {
        this.message = String.format("Parsing error: %s", message);
    }

    ParseException(String message, int position) {
        this.message = String.format("Parsing error at position %d: %s", position, message);
    }

    ParseException(String message, int position, Throwable cause) {
        super(cause);
        this.message = String.format("Parsing error at position %d: %s", position, message);
    }

    ParseException(Token actualToken, TokenType... expectedTokens) {

        this.message = String.format("Unexpected token '%s' at position %d. Expected one of: %s",
                actualToken.getLiteral(), actualToken.getPosition(), buildString(expectedTokens));
    }

    @Override
    public String getMessage() {
        return message;
    }

    private static String buildString(TokenType... tokenTypes) {

        if (tokenTypes == null || tokenTypes.length == 0) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        int len = tokenTypes.length;
        for (int i = 0; i < len; i++) {
            builder.append(Scanner.getTokenLiteral(tokenTypes[i]));
            if (i < len - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}
