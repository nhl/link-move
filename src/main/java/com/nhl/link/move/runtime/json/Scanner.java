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
                throw new RuntimeException("Unexpected error", e);
            }
        }

        void unread(int c) {
            try {
                currentIndex--;
                reader.unread(c);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected error", e);
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
            throw new RuntimeException("No more tokens to read");
        }

        TokenBuilder builder = new TokenBuilder(reader.getCurrentIndex());

        int c;
        try {
            while ((c = reader.read()) != -1 && builder.acceptCharacter((char) c))
                ;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error at position " + reader.getCurrentIndex(), e);
        }

        if (c == -1) {
            isEOF = true;
        } else {
            reader.unread(c);
        }

        return builder.build();
    }

    private static class TokenBuilder {

        private int startPosition;
        private StringBuilder buffer; // used to store identifiers
        private Type tokenType;
        private int lastSeen;

        TokenBuilder(int startPosition) {
            this.startPosition = startPosition;
        }

        private boolean isEmpty() {
            return buffer == null;
        }

        private void append(int c) {
            if (buffer == null) {
                buffer = new StringBuilder();
            }
            buffer.append((char) c);
        }

        boolean acceptCharacter(char c) throws Exception {

            boolean accepted = doAccept(c);
            lastSeen = c;
            return accepted;
        }

        private boolean doAccept(char c) throws Exception {

            if (Character.isWhitespace(c)) {
                // ignore leading whitespaces,
                // do not accept trailing whitespaces
                if (isEmpty()) {
                    startPosition++;
                    return true;
                } else {
                    return false;
                }
            }

            if (tokenType == Type.CHILD_ACCESS && c == '.') {
                tokenType = Type.RECURSIVE_DESCENT;
                return true;
            }

            if (tokenType == Type.PREDICATE_START) {
                if (c == '(') {
                    if (lastSeen == '(') {
                        throw new RuntimeException("Unexpected character '('");
                    } else {
                        return true;
                    }
                } else {
                    throw new RuntimeException("Expected '(' but received: " + c);
                }
            }

            // token type is already known,
            // will not accept anymore
            if (isEmpty() && tokenType != null) {
                return false;
            }

            if (!isEmpty()) {
                // current token is identifier,
                // reserved character belongs to next token
                if (isReserved(c)) {
                    return false;
                } else {
                    append(c);
                    return true;
                }
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
                default: {
                    tokenType = Type.IDENTIFIER;
                    append(c);
                    return true;
                }
            }
        }

        private boolean isReserved(char c) {
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
                    return true;
                default:
                    return false;
            }
        }

        Token build() {

            String literal;
            switch (tokenType) {
                case ROOT_NODE_REF: {
                    literal = "$";
                    break;
                }
                case CURRENT_NODE_REF: {
                    literal = "@";
                    break;
                }
                case CHILD_ACCESS: {
                    literal = ".";
                    break;
                }
                case RECURSIVE_DESCENT: {
                    literal = "..";
                    break;
                }
                case WILDCARD: {
                    literal = "*";
                    break;
                }
                case UNION: {
                    literal = ",";
                    break;
                }
                case FILTER_START: {
                    literal = "[";
                    break;
                }
                case FILTER_END: {
                    literal = "]";
                    break;
                }
                case PREDICATE_START: {
                    literal = "?(";
                    break;
                }
                case PREDICATE_END: {
                    literal = ")";
                    break;
                }
                case IDENTIFIER: {
                    literal = buffer.toString();
                    break;
                }
                default: {
                    throw new RuntimeException("Unknown token type: " + tokenType.name());
                }
            }

            return new Token(tokenType, literal, startPosition);
        }
    }
}
