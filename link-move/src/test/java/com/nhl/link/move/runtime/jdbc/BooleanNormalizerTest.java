package com.nhl.link.move.runtime.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BooleanNormalizerTest {

    private static BooleanNormalizer normalizer = new BooleanNormalizer();

    @Test
    public void testNormalize_Null() {
        assertNull(normalizer.normalize(null, null));
    }

    @Test
    public void testNormalize_Byte() {
        assertEquals(true, normalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalize_Short() {
        assertEquals(true, normalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalize_Integer() {
        assertEquals(true, normalizer.normalize(1, null));
    }

    @Test
    public void testNormalize_Long() {
        assertEquals(true, normalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalize_Boolean() {
        assertEquals(true, normalizer.normalize(true, null));
    }
}
