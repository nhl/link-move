package com.nhl.link.move.runtime.json;

import org.junit.Before;
import org.junit.Test;

public class QueryCompilerTest {

    private QueryCompiler compiler;

    @Before
    public void setUp() {
        compiler = new QueryCompiler();
    }

    @Test(expected = Exception.class)
    public void testCompile_Empty() {
        compiler.compile("");
    }

    @Test
    public void testCompile_Query1() {
        compiler.compile("$.store.book[*].author");
    }
}
