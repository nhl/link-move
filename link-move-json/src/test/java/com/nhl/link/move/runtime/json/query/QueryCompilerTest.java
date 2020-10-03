package com.nhl.link.move.runtime.json.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryCompilerTest {

    private QueryCompiler compiler;

    @BeforeEach
    public void setUp() {
        compiler = new QueryCompiler();
    }

    @Test
    public void testCompile_Empty() {
        assertThrows(Exception.class, () -> compiler.compile(""));
    }

    @Test
    public void testCompile_Query1() {
        compiler.compile("$.store.book[*].author");
    }

    @Test
    public void testCompile_Query2() {
        assertThrows(Exception.class, () -> compiler.compile("$.store.book['*].author"));
    }

    @Test
    public void testCompile_Query3() {
        assertThrows(Exception.class, () -> compiler.compile("$.store.boo k[*].author"));
    }

    @Test
    public void testCompile_Query4() {
        assertThrows(Exception.class, () -> compiler.compile("$store.book[*].author"));
    }

    @Test
    public void testCompile_Query5() {
        compiler.compile("@.store.book[*].author");
    }

    @Test
    public void testCompile_Query6() {
        assertThrows(Exception.class, () -> compiler.compile("$.store.\"book\"[*].author"));
    }

    @Test
    public void testCompile_Query7() {
        compiler.compile("$.store.book[*][\"author\"]");
    }

    @Test
    public void testCompile_Query8() {
        assertThrows(Exception.class, () -> compiler.compile("$.store.[book][*].author"));
    }

    @Test
    public void testCompile_Query9() {
        compiler.compile("$.store.book[*].readers[1]");
    }

    @Test
    public void testCompile_Query10() {
        compiler.compile("$.store.book[*].readers.1");
    }

    @Test
    public void testCompile_Query11() {
        assertThrows(Exception.class, () -> compiler.compile("$.store.book[*.readers.1"));
    }

    @Test
    public void testCompile_Query12() {
        assertThrows(Exception.class, () -> compiler.compile("$...store"));
    }

    @Test
    public void testCompile_Query13() {
        compiler.compile("$..[('store')].book");
    }

    @Test
    public void testCompile_Query14() {
        assertThrows(Exception.class, () -> compiler.compile("$..[('store')].book)"));
    }

    @Test
    public void testCompile_Query15() {
        assertThrows(Exception.class, () -> compiler.compile("$.store..readers[?(@.name == 'Bob' &&)]"));
    }

    @Test
    public void testCompile_Query16() {
        assertThrows(Exception.class, () -> compiler.compile("$.store..readers[?(@.name == 'Bob' @.age == 60)]"));
    }

    @Test
    public void testCompile_Query17() {
        assertThrows(Exception.class, () -> compiler.compile("$.store..readers[?( == 'Bob' && @.age == 60)]"));
    }

    @Test
    public void testCompile_Query18() {
        assertThrows(Exception.class, () -> compiler.compile("$.store..readers[?(@.name == 'Bob' && (@.age == 60)]"));
    }
}
