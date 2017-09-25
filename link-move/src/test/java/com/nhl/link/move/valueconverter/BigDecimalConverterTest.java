package com.nhl.link.move.valueconverter;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BigDecimalConverterTest {

    private static final BigDecimalConverter CONVERTER = new BigDecimalConverter();

    @Test
    public void testConvert_Null() {
        assertNull(CONVERTER.convert(null));
    }

    @Test
    public void testConvert_EmptyString() {
        assertNull(CONVERTER.convert(""));
    }

    @Test
    public void testConvert_String() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert("1"));
    }

    @Test
    public void testConvert_Byte() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((byte) 1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((short) 1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((long) 1));
    }

    @Test
    public void testConvert_Float() {
        assertEquals(BigDecimal.valueOf(1d), CONVERTER.convert((float) 1));
    }

    @Test
    public void testConvert_Double() {
        assertEquals(BigDecimal.valueOf(1d), CONVERTER.convert((double) 1));
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(BigInteger.ONE));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(BigDecimal.ONE));
    }

}
