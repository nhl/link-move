package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void testConvert_Long_TooLarge1() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Long.valueOf(Integer.MAX_VALUE) + 1));
    }

    @Test
    public void testConvert_Long_TooLarge2() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Long.valueOf(Integer.MIN_VALUE) - 1));
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigInteger.ONE));
    }

    @Test
    public void testConvert_BigInteger_TooLarge1() {
        BigInteger x = BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x));
    }

    @Test
    public void testConvert_BigInteger_TooLarge2() {
        BigInteger x = BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigDecimal.ONE));
    }

    @Test
    public void testConvert_BigDecimal_TooLarge1() {
        BigDecimal x = BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x));
    }

    @Test
    public void testConvert_BigDecimal_TooLarge2() {
        BigDecimal x = BigDecimal.valueOf(Integer.MIN_VALUE).subtract(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x));
    }

    @Test
    public void testConvert_BigDecimal_NonZeroFractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(BigDecimal.valueOf(1.1)));
    }
}
