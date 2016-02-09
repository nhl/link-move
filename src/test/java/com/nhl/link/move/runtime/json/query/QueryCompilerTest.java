package com.nhl.link.move.runtime.json.query;

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

    @Test(expected = Exception.class)
    public void testCompile_Query2() {
        compiler.compile("$.store.book['*].author");
    }

    @Test(expected = Exception.class)
    public void testCompile_Query3() {
        compiler.compile("$.store.boo k[*].author");
    }

    @Test(expected = Exception.class)
    public void testCompile_Query4() {
        compiler.compile("$store.book[*].author");
    }

    @Test(expected = Exception.class)
    public void testCompile_Query5() {
        compiler.compile("@.store.book[*].author");
    }

    @Test(expected = Exception.class)
    public void testCompile_Query6() {
        compiler.compile("$.store.\"book\"[*].author");
    }

    @Test
    public void testCompile_Query7() {
        compiler.compile("$.store.book[*][\"author\"]");
    }

    @Test(expected = Exception.class)
    public void testCompile_Query8() {
        compiler.compile("$.store.[book][*].author");
    }

    @Test
    public void testCompile_Query9() {
        compiler.compile("$.store.book[*].readers[1]");
    }

    @Test
    public void testCompile_Query10() {
        compiler.compile("$.store.book[*].readers.1");
    }
}
