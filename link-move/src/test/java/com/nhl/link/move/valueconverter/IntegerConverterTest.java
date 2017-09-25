package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IntegerConverterTest {

    private static final IntegerConverter CONVERTER = new IntegerConverter();

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
        assertEquals(Integer.valueOf(1), CONVERTER.convert("1"));
    }

    @Test
    public void testConvert_Byte() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((byte) 1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((short) 1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((long) 1));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_Long_TooLarge1() {
        CONVERTER.convert(Long.valueOf(Integer.MAX_VALUE) + 1);
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_Long_TooLarge2() {
        CONVERTER.convert(Long.valueOf(Integer.MIN_VALUE) - 1);
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigInteger.ONE));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_BigInteger_TooLarge1() {
        CONVERTER.convert(BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_BigInteger_TooLarge2() {
        CONVERTER.convert(BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.ONE));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigDecimal.ONE));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_BigDecimal_TooLarge1() {
        CONVERTER.convert(BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_BigDecimal_TooLarge2() {
        CONVERTER.convert(BigDecimal.valueOf(Integer.MIN_VALUE).subtract(BigDecimal.ONE));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_BigDecimal_NonZeroFractionalPart() {
        CONVERTER.convert(BigDecimal.valueOf(1.1));
    }
}
