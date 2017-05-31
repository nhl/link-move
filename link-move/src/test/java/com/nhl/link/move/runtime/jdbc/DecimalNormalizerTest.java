package com.nhl.link.move.runtime.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DecimalNormalizerTest {

    private static final DecimalNormalizer normalizer = new DecimalNormalizer();

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
        assertEquals(BigDecimal.ONE, normalizer.normalize("1", null));
    }

    @Test
    public void testNormalize_Byte() {
        assertEquals(BigDecimal.ONE, normalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalize_Short() {
        assertEquals(BigDecimal.ONE, normalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalize_Integer() {
        assertEquals(BigDecimal.ONE, normalizer.normalize(1, null));
    }

    @Test
    public void testNormalize_Long() {
        assertEquals(BigDecimal.ONE, normalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalize_Float() {
        assertEquals(BigDecimal.valueOf(1d), normalizer.normalize((float) 1, null));
    }

    @Test
    public void testNormalize_Double() {
        assertEquals(BigDecimal.valueOf(1d), normalizer.normalize((double) 1, null));
    }

    @Test
    public void testNormalize_BigInteger() {
        assertEquals(BigDecimal.ONE, normalizer.normalize(BigInteger.ONE, null));
    }

    @Test
    public void testNormalize_BigDecimal() {
        assertEquals(BigDecimal.ONE, normalizer.normalize(BigDecimal.ONE, null));
    }

}
