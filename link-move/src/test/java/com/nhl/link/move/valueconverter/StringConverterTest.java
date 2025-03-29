package com.nhl.link.move.valueconverter;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringConverterTest {

    private static final StringConverter CONVERTER = new StringConverter();

    @Test
    public void convert_Byte() {
        assertEquals("1", CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void convert_Short() {
        assertEquals("1", CONVERTER.convert((short) 1, -1));
    }

    @Test
    public void convert_Integer() {
        assertEquals("1", CONVERTER.convert(1, -1));
    }

    @Test
    public void convert_Long() {
        assertEquals("1", CONVERTER.convert(1L, -1));
    }

    @Test
    public void convert_BigInteger() {
        assertEquals("1", CONVERTER.convert(BigInteger.ONE, -1));
    }

    @Test
    public void convert_Float() {
        assertEquals("1.0", CONVERTER.convert(1f, -1));
    }

    @Test
    public void convert_Float_NaN() {
        assertEquals("NaN", CONVERTER.convert(Float.NaN, -1));
    }

    @Test
    public void convert_Float_PositiveInfinity() {
        assertEquals("Infinity", CONVERTER.convert(Float.POSITIVE_INFINITY, -1));
    }

    @Test
    public void convert_Float_NegativeInfinity() {
        assertEquals("-Infinity", CONVERTER.convert(Float.NEGATIVE_INFINITY, -1));
    }

    @Test
    public void convert_Float_FractionalPart() {
        assertEquals("1.1", CONVERTER.convert(1.1f, -1));
    }

    @Test
    public void convert_Double() {
        assertEquals("1.0", CONVERTER.convert(1d, -1));
    }

    @Test
    public void convert_Double_NaN() {
        assertEquals("NaN", CONVERTER.convert(Double.NaN, -1));
    }

    @Test
    public void convert_Double_PositiveInfinity() {
        assertEquals("Infinity", CONVERTER.convert(Double.POSITIVE_INFINITY, -1));
    }

    @Test
    public void convert_Double_NegativeInfinity() {
        assertEquals("-Infinity", CONVERTER.convert(Double.NEGATIVE_INFINITY, -1));
    }

    @Test
    public void convert_Double_FractionalPart() {
        assertEquals("1.1", CONVERTER.convert(1.1d, -1));
    }

    @Test
    public void convert_BigDecimal() {
        assertEquals("1", CONVERTER.convert(BigDecimal.ONE, -1));
    }

    @Test
    public void convert_BigDecimal_Exception() {
        assertEquals("1.1", CONVERTER.convert(new BigDecimal("1.1"), -1));
    }

    @Test
    public void convert_Null() {
        assertEquals(null, CONVERTER.convert(null, -1));
    }
}
