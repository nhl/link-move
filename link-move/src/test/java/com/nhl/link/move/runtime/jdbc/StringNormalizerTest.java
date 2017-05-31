package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class StringNormalizerTest {

    private static final StringNormalizer normalizer = new StringNormalizer();

    @Test
    public void testNormalize_Byte() {
        assertEquals("1", normalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalize_Short() {
        assertEquals("1", normalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalize_Integer() {
        assertEquals("1", normalizer.normalize(1, null));
    }

    @Test
    public void testNormalize_Long() {
        assertEquals("1", normalizer.normalize(1L, null));
    }

    @Test
    public void testNormalize_BigInteger() {
        assertEquals("1", normalizer.normalize(BigInteger.ONE, null));
    }

    @Test
    public void testNormalize_Float() {
        assertEquals("1", normalizer.normalize(1f, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Float_NaN() {
        normalizer.normalize(Float.NaN, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Float_PositiveInfinity() {
        normalizer.normalize(Float.POSITIVE_INFINITY, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Float_NegativeInfinity() {
        normalizer.normalize(Float.NEGATIVE_INFINITY, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Float_FractionalPart() {
        normalizer.normalize(1.1f, null);
    }

    @Test
    public void testNormalize_Double() {
        assertEquals("1", normalizer.normalize(1d, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Double_NaN() {
        normalizer.normalize(Double.NaN, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Double_PositiveInfinity() {
        normalizer.normalize(Double.POSITIVE_INFINITY, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Double_NegativeInfinity() {
        normalizer.normalize(Double.NEGATIVE_INFINITY, null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_Double_FractionalPart() {
        normalizer.normalize(1.1d, null);
    }

    @Test
    public void testNormalize_BigDecimal() {
        assertEquals("1", normalizer.normalize(BigDecimal.ONE, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_BigDecimal_Exception() {
        normalizer.normalize(BigDecimal.valueOf(1.1d), null);
    }

    @Test
    public void testNormalize_Null() {
        assertEquals(null, normalizer.normalize(null, null));
    }
}
