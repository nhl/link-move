package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ArrayDataRowTest {

    private Index index;

    @Before
    public void initIndex() {
        this.index = new Index("a", "b");
    }

    @Test
    public void testToString() {
        ArrayDataRow dr = new ArrayDataRow(index, "ABC", 1);
        assertEquals("" +
                "ArrayDataRow" + System.lineSeparator() +
                "a   b" + System.lineSeparator() +
                "--- -" + System.lineSeparator() +
                "ABC 1", dr.toString());
    }
}
