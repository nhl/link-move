package com.nhl.link.move.valueconverter;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BigDecimalConverterTest {

    private static final BigDecimalConverter CONVERTER = new BigDecimalConverter();

    @Test
    public void convert_Null() {
        assertNull(CONVERTER.convert(null, -1));
    }

    @Test
    public void convert_EmptyString() {
        assertNull(CONVERTER.convert("", -1));
    }

    @Test
    public void convert_String() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert("1", -1));
    }

    @Test
    public void convert_Byte() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void convert_Short() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((short) 1, -1));
    }

    @Test
    public void convert_Integer() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(1, -1));
    }

    @Test
    public void convert_Long() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert((long) 1, -1));
    }

    @Test
    public void convert_Float() {
        assertEquals(BigDecimal.valueOf(1d), CONVERTER.convert((float) 1, -1));
    }

    @Test
    public void convert_Double() {
        assertEquals(BigDecimal.valueOf(1d), CONVERTER.convert((double) 1, -1));
    }

    @Test
    public void convert_BigInteger() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(BigInteger.ONE, -1));
    }

    @Test
    public void convert_BigDecimal() {
        assertEquals(BigDecimal.ONE, CONVERTER.convert(BigDecimal.ONE, -1));
    }

}
