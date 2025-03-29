package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

public class IntegerConverterTest {

    private static final IntegerConverter CONVERTER = new IntegerConverter();

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
        assertEquals(Integer.valueOf(1), CONVERTER.convert("1", -1));
    }

    @Test
    public void convert_Byte() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void convert_Short() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((short) 1, -1));
    }

    @Test
    public void convert_Integer() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(1, -1));
    }

    @Test
    public void convert_Long() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert((long) 1, -1));
    }

    @Test
    public void convert_Long_TooLarge1() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Long.valueOf(Integer.MAX_VALUE) + 1, -1));
    }

    @Test
    public void convert_Long_TooLarge2() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(Long.valueOf(Integer.MIN_VALUE) - 1, -1));
    }

    @Test
    public void convert_BigInteger() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigInteger.ONE, -1));
    }

    @Test
    public void convert_BigInteger_TooLarge1() {
        BigInteger x = BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void convert_BigInteger_TooLarge2() {
        BigInteger x = BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void convert_BigDecimal() {
        assertEquals(Integer.valueOf(1), CONVERTER.convert(BigDecimal.ONE, -1));
    }

    @Test
    public void convert_BigDecimal_TooLarge1() {
        BigDecimal x = BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void convert_BigDecimal_TooLarge2() {
        BigDecimal x = BigDecimal.valueOf(Integer.MIN_VALUE).subtract(BigDecimal.ONE);
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(x, -1));
    }

    @Test
    public void convert_BigDecimal_NonZeroFractionalPart() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(BigDecimal.valueOf(1.1), -1));
    }
}
