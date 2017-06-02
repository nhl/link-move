package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LongNormalizerTest {

    private static final LongNormalizer normalizer = new LongNormalizer();

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
        assertEquals(Long.valueOf(1), normalizer.normalize("1", null));
    }

    @Test
    public void testNormalize_Byte() {
        assertEquals(Long.valueOf(1), normalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalize_Short() {
        assertEquals(Long.valueOf(1), normalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalize_Integer() {
        assertEquals(Long.valueOf(1), normalizer.normalize(1, null));
    }

    @Test
    public void testNormalize_Long() {
        assertEquals(Long.valueOf(1), normalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalize_BigInteger() {
        assertEquals(Long.valueOf(1), normalizer.normalize(BigInteger.ONE, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigInteger_TooLarge1() {
        normalizer.normalize(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigInteger_TooLarge2() {
        normalizer.normalize(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), null);
    }

    @Test
    public void testNormalize_BigDecimal() {
        assertEquals(Long.valueOf(1), normalizer.normalize(BigDecimal.ONE, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigDecimal_TooLarge1() {
        normalizer.normalize(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigDecimal_TooLarge2() {
        normalizer.normalize(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigDecimal_NonZeroFractionalPart() {
        normalizer.normalize(BigDecimal.valueOf(1.1), null);
    }
}
