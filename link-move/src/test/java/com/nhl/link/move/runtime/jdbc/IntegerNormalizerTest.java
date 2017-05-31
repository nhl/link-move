package com.nhl.link.move.runtime.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IntegerNormalizerTest {

    private static final IntegerNormalizer normalizer = new IntegerNormalizer();

    @Test
    public void testNormalize_Null() {
        assertNull(normalizer.normalize(null, null));
    }

    @Test
    public void testNormalize_EmptyString() {
        assertNull(normalizer.normalize("", null));
    }

    @Test
    public void testNormalize_String() {
        assertEquals(Integer.valueOf(1), normalizer.normalize("1", null));
    }

    @Test
    public void testNormalize_Byte() {
        assertEquals(Integer.valueOf(1), normalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalize_Short() {
        assertEquals(Integer.valueOf(1), normalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalize_Integer() {
        assertEquals(Integer.valueOf(1), normalizer.normalize(1, null));
    }

    @Test
    public void testNormalize_Long() {
        assertEquals(Integer.valueOf(1), normalizer.normalize((long) 1, null));
    }

}
