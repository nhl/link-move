package com.nhl.link.move.runtime.json.query;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ScannerTest {

    @Test
    public void testScanning_EmptyPath() {
        List<Token> tokens = tokenizeQuery("");
        assertEquals(0, tokens.size());
    }

    @Test
    public void testScanning_Query1() {
        List<Token> tokens = tokenizeQuery("$.store.book[*].author");
        assertEquals(10, tokens.size());
    }

    private static List<Token> tokenizeQuery(String query) {

        List<Token> tokens = new ArrayList<>();
        Scanner scanner = new Scanner(query);
        while (scanner.hasNext()) {
            tokens.add(scanner.nextToken());
        }
        return tokens;
    }
}
