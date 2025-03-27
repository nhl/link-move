package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringConverterTest {

    private static final StringConverter CONVERTER = new StringConverter();

    @Test
    public void testConvert_Byte() {
        assertEquals("1", CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals("1", CONVERTER.convert((short) 1, -1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals("1", CONVERTER.convert(1, -1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals("1", CONVERTER.convert(1L, -1));
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals("1", CONVERTER.convert(BigInteger.ONE, -1));
    }

    @Test
    public void testConvert_Float() {
        assertEquals("1", CONVERTER.convert(1f, -1));
    }

    @Test
    public void testConvert_Float_NaN() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.NaN, -1));
    }

    @Test
    public void testConvert_Float_PositiveInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.POSITIVE_INFINITY, -1));
    }

    @Test
    public void testConvert_Float_NegativeInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.NEGATIVE_INFINITY, -1));
    }

    @Test
    public void testConvert_Float_FractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(1.1f, -1));
    }

    @Test
    public void testConvert_Double() {
        assertEquals("1", CONVERTER.convert(1d, -1));
    }

    @Test
    public void testConvert_Double_NaN() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.NaN, -1));
    }

    @Test
    public void testConvert_Double_PositiveInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.POSITIVE_INFINITY, -1));
    }

    @Test
    public void testConvert_Double_NegativeInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.NEGATIVE_INFINITY, -1));
    }

    @Test
    public void testConvert_Double_FractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(1.1d, -1));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals("1", CONVERTER.convert(BigDecimal.ONE, -1));
    }

    @Test
    public void testConvert_BigDecimal_Exception() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(BigDecimal.valueOf(1.1d), -1));
    }

    @Test
    public void testConvert_Null() {
        assertEquals(null, CONVERTER.convert(null, -1));
    }
}
