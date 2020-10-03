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
        assertEquals("1", CONVERTER.convert((byte) 1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals("1", CONVERTER.convert((short) 1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals("1", CONVERTER.convert(1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals("1", CONVERTER.convert(1L));
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals("1", CONVERTER.convert(BigInteger.ONE));
    }

    @Test
    public void testConvert_Float() {
        assertEquals("1", CONVERTER.convert(1f));
    }

    @Test
    public void testConvert_Float_NaN() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.NaN));
    }

    @Test
    public void testConvert_Float_PositiveInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.POSITIVE_INFINITY));
    }

    @Test
    public void testConvert_Float_NegativeInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Float.NEGATIVE_INFINITY));
    }

    @Test
    public void testConvert_Float_FractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(1.1f));
    }

    @Test
    public void testConvert_Double() {
        assertEquals("1", CONVERTER.convert(1d));
    }

    @Test
    public void testConvert_Double_NaN() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.NaN));
    }

    @Test
    public void testConvert_Double_PositiveInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testConvert_Double_NegativeInfinity() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testConvert_Double_FractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(1.1d));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals("1", CONVERTER.convert(BigDecimal.ONE));
    }

    @Test
    public void testConvert_BigDecimal_Exception() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(BigDecimal.valueOf(1.1d)));
    }

    @Test
    public void testConvert_Null() {
        assertEquals(null, CONVERTER.convert(null));
    }
}
