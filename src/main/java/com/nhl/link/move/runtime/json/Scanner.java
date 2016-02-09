package com.nhl.link.move.runtime.json;

import com.nhl.link.move.runtime.json.Token.Type;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;

class Scanner {

    private final CountingPushbackReader reader;
    private boolean isEOF;

    Scanner(String query) {
        reader = new CountingPushbackReader(new PushbackReader(new StringReader(query)));
    }

    private static class CountingPushbackReader {

        private final PushbackReader reader;
        private int currentIndex;

        CountingPushbackReader(PushbackReader reader) {
            this.reader = reader;
        }

        int read() {
            try {
                currentIndex++;
                return reader.read();
            } catch (IOException e) {
                throw new RuntimeException("Unexpected I/O error", e);
            }
        }

        void unread(int c) {
            try {
                currentIndex--;
                reader.unread(c);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected I/O error", e);
            }
        }

        int getCurrentIndex() {
            return currentIndex;
        }
    }

    boolean hasNext() {
        if (isEOF) {
            return false;
        }
        int c = reader.read();
        if (c == -1) {
            isEOF = true;
        } else {
            reader.unread(c);
        }
        return !isEOF;
    }

    Token nextToken() {

        if (isEOF) {
            throw new ParseException("No more tokens to read", reader.getCurrentIndex());
        }

        TokenBuilder builder = new TokenBuilder(reader.getCurrentIndex());
        try {
            int c;
            while ((c = reader.read()) != -1 && builder.acceptCharacter((char) c))
                ;

            if (c == -1) {
                isEOF = true;
            } else {
                reader.unread(c);
            }

            return builder.build();

        } catch (Exception e) {
            throw new ParseException(e.getMessage(), reader.getCurrentIndex());
        }
    }

    private static class TokenBuilder {

        private static class IdentifierBuilder {

            private StringBuilder buffer; // used to store identifier name
            private boolean isQuoted;
            private boolean isComplete;

            IdentifierBuilder(boolean isQuoted) {
                this.isQuoted = isQuoted;
            }

            boolean acceptCharacter(char c) {

                if (isComplete) {
                    return false;
                }

                // TODO: support backslash escape in quoted identifiers
                if (isQuoted && isQuote(c)) {
                    isComplete = true;
                    return true;
                } else if (!isQuoted && isReserved(c)) {
                    return false;
                } else if (Character.isWhitespace(c)) {
                    throw new RuntimeException("Whitespace in unquoted identifier");
                }

                if (buffer == null) {
                    buffer = new StringBuilder();
                }
                buffer.append(c);
                return true;
            }

            String build() {

                if (buffer == null || buffer.length() == 0) {
                    throw new RuntimeException("Empty identifier");
                }
                if (isQuoted && !isComplete) {
                    throw new RuntimeException("Unmatched quote in identifier");
                }
                return buffer.toString();
            }
        }

        private int startPosition;
        private IdentifierBuilder identifierBuilder;
        private Type tokenType;
        private int lastSeen;

        TokenBuilder(int startPosition) {
            this.startPosition = startPosition;
        }

        boolean acceptCharacter(char c) throws Exception {

            boolean accepted = doAccept(c);
            lastSeen = c;
            return accepted;
        }

        private boolean doAccept(char c) throws Exception {

            if (tokenType == null && Character.isWhitespace(c)) {
                startPosition++;
                return true;
            }

            if (tokenType == Type.CHILD_ACCESS && c == '.') {
                tokenType = Type.RECURSIVE_DESCENT;
                return true;
            }

            if (tokenType == Type.PREDICATE_START) {
                if (c == '(') {
                    return lastSeen == '?';
                }
            }

            // token type is already known,
            // will not accept anymore
            if (identifierBuilder == null && tokenType != null) {
                return false;
            }

            if (identifierBuilder != null) {
                boolean accepted =  identifierBuilder.acceptCharacter(c);
                if (accepted && tokenType == Type.NUMERIC_VALUE && !Character.isDigit(c)) {
                    tokenType = Type.IDENTIFIER;
                }
                return accepted;
            }

            // prediction block -
            // if we're here, then current token's type is unknown yet
            switch (c) {
                case '$': {
                    tokenType = Type.ROOT_NODE_REF;
                    return true;
                }
                case '@': {
                    tokenType = Type.CURRENT_NODE_REF;
                    return true;
                }
                case '.': {
                    tokenType = Type.CHILD_ACCESS;
                    return true;
                }
                case '*': {
                    tokenType = Type.WILDCARD;
                    return true;
                }
                case '[': {
                    tokenType = Type.FILTER_START;
                    return true;
                }
                case ']': {
                    tokenType = Type.FILTER_END;
                    return true;
                }
                case '?': {
                    tokenType = Type.PREDICATE_START;
                    return true;
                }
                case ')': {
                    tokenType = Type.PREDICATE_END;
                    return true;
                }
                case ',': {
                    tokenType = Type.UNION;
                    return true;
                }
                case '\'':
                case '\"': {
                    tokenType = Type.QUOTED_IDENTIFIER;
                    identifierBuilder = new IdentifierBuilder(true);
                    return true;
                }
                default: {
                    if (Character.isDigit(c)) {
                        tokenType = Type.NUMERIC_VALUE;
                    } else {
                        tokenType = Type.IDENTIFIER;
                    }
                    identifierBuilder = new IdentifierBuilder(false);
                    identifierBuilder.acceptCharacter(c);
                    return true;
                }
            }
        }

        private static boolean isQuote(char c) {
            return c == '\'' || c == '\"';
        }

        private static boolean isReserved(char c) {
            switch (c) {
                case '$':
                case '@':
                case '.':
                case '*':
                case '[':
                case ']':
                case '?':
                case ')':
                case ',':
                case '\'':
                case '\"':
                    return true;
                default:
                    return false;
            }
        }

        Token build() {

            String literal;
            if (tokenType == Type.IDENTIFIER || tokenType == Type.QUOTED_IDENTIFIER
                    || tokenType == Type.NUMERIC_VALUE) {

                literal = identifierBuilder.build();
            } else {
                literal = getTokenLiteral(tokenType);
            }

            return new Token(tokenType, literal, startPosition);
        }
    }

    static String getTokenLiteral(Type tokenType) {

        switch (tokenType) {
            case ROOT_NODE_REF: {
                return "$";
            }
            case CURRENT_NODE_REF: {
                return "@";
            }
            case CHILD_ACCESS: {
                return ".";
            }
            case RECURSIVE_DESCENT: {
                return "..";
            }
            case WILDCARD: {
                return "*";
            }
            case UNION: {
                return ",";
            }
            case FILTER_START: {
                return "[";
            }
            case FILTER_END: {
                return "]";
            }
            case PREDICATE_START: {
                return "?(";
            }
            case PREDICATE_END: {
                return ")";
            }
            case NUMERIC_VALUE: {
                return "numeric";
            }
            case IDENTIFIER: {
                return "identifier";
            }
            case QUOTED_IDENTIFIER: {
                return "\"";
            }
            default: {
                throw new RuntimeException("Unknown token type: " + tokenType.name());
            }
        }
    }
}
