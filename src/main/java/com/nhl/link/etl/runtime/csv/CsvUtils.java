package com.nhl.link.etl.runtime.csv;

import java.io.IOException;
import java.io.StringReader;

/**
 * @since 1.4
 */
public class CsvUtils {

    public static String[] parse(String[] result, String data, char delimiter) throws IOException {
        StringReader reader = new StringReader(data);

        int c;
        char current;
        final char QUOTE = '"';
        int fieldIdx = 0;
        boolean isFirstCharInField = true, // each CSV line begins with a field
                isQuotedField = false,
                isExpectingQuote = false;
        StringBuilder field = new StringBuilder();
        StringBuilder whitespaces = new StringBuilder();
        while ((c = reader.read()) != -1) {
            current = (char) c;
            switch (current) {
                case QUOTE: {
                    if (isFirstCharInField) {
                        isQuotedField = true;
                        whitespaces = new StringBuilder();
                    } else if (isQuotedField) {
                        if (isExpectingQuote) {
                            if (whitespaces.length() > 0) {
                                throw new RuntimeException("Unexpected whitespaces between in-field quotes");
                            }
                            field.append(current);
                        } else {
                            field.append(whitespaces.toString());
                            whitespaces = new StringBuilder();
                        }
                        isExpectingQuote = !isExpectingQuote;
                    }
                    break;
                }
                default: {
                    if (current == delimiter) {

                        if (!isExpectingQuote) {
                            field.append(whitespaces.toString());
                            whitespaces = new StringBuilder();
                        }

                        if (isQuotedField && !isExpectingQuote) {
                            field.append(current);
                        } else {
                            result[fieldIdx] = field.toString();
                            if (++fieldIdx >= result.length) {
                                throw new RuntimeException("Unexpectedly reached end of result array before reading entire data");
                            }
                            field = new StringBuilder();
                            whitespaces = new StringBuilder();
                            isFirstCharInField = true;
                            isExpectingQuote = false;
                            continue;
                        }
                    } else if (Character.isWhitespace(current)) {
                            whitespaces.append(current);
                            continue;
                    } else {
                        if (isExpectingQuote) {
                            throw new RuntimeException("Expected delimiter or quote, but got some other character");
                        } else {
                            if (whitespaces.length() > 0) {
                                field.append(whitespaces.toString());
                                whitespaces = new StringBuilder();
                            }
                            field.append(current);
                        }
                    }
                    break;
                }
            }

            if (!Character.isWhitespace(current)) {
                isFirstCharInField = false;
            }
        }

        if (!isExpectingQuote) {
            field.append(whitespaces.toString());
        }
        result[fieldIdx] = field.toString();

        return result;
    }

}
