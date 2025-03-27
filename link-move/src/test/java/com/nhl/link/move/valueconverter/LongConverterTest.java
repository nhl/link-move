package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

public class LongConverterTest {

    private static final LongConverter CONVERTER = new LongConverter();

    @Test
    public void testConvert_Null() {
        assertNull(CONVERTER.convert(null, -1));
    }

    @Test
    public void testConvert_EmptyString() {
        assertNull(CONVERTER.convert("", -1));
    }

    @Test
    public void testConvert_String() {
        assertEquals(1L, CONVERTER.convert("1", -1));
    }

    @Test
    public void testConvert_Byte() {
        assertEquals(1L, CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals(1L, CONVERTER.convert((short) 1, -1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals(1L, CONVERTER.convert(1, -1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals(1L, CONVERTER.convert((long) 1, -1));
    }

    @Test
    public void testConvert_BigInteger() {
        assertEquals(1L, CONVERTER.convert(BigInteger.ONE, -1));
    }

    @Test
    public void testConvert_BigInteger_TooLarge1() {
        assertThrows(LmRuntimeException.class,
                () -> CONVERTER.convert(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), -1));
    }

    @Test
    public void testConvert_BigInteger_TooLarge2() {
        BigInteger x = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void testConvert_BigDecimal() {
        assertEquals(1L, CONVERTER.convert(BigDecimal.ONE, -1));
    }

    @Test
    public void testConvert_BigDecimal_TooLarge1() {
        BigDecimal x = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void testConvert_BigDecimal_TooLarge2() {
        BigDecimal x = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void testConvert_BigDecimal_NonZeroFractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(BigDecimal.valueOf(1.1), -1));
    }
}
