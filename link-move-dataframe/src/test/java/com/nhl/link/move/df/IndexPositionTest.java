package com.nhl.link.move.df;

import org.junit.Test;

import static org.junit.Assert.*;

public class IndexPositionTest {

    @Test
    public void testEquals() {
        IndexPosition ip1 = new IndexPosition(1, "a");
        IndexPosition ip2 = new IndexPosition(1, "a");
        IndexPosition ip3 = new IndexPosition(1, "b");
        IndexPosition ip4 = new IndexPosition(2, "a");
        IndexPosition ip5 = new IndexPosition(2, "b");

        assertTrue(ip1.equals(ip1));
        assertTrue(ip1.equals(ip2));
        assertFalse(ip1.equals(ip3));
        assertFalse(ip1.equals(ip4));
        assertFalse(ip1.equals(ip5));
    }
}
