package com.nhl.link.move.runtime.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NormalizerTest {

    private static BigIntNormalizer longNormalizer;
    private static BooleanNormalizer booleanNormalizer;
    private static IntegerNormalizer integerNormalizer;
    private static DecimalNormalizer decimalNormalizer;

    @BeforeClass
    public static void setUp() {
        longNormalizer = new BigIntNormalizer();
        booleanNormalizer = new BooleanNormalizer();
        decimalNormalizer = new DecimalNormalizer();
        integerNormalizer = new IntegerNormalizer();
    }

    @Test
    public void testNormalizer_Null_To_Long() {
        assertNull(longNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_Long() {
        assertNull(longNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Null_To_Boolean() {
        assertNull(booleanNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_Byte_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Boolean_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize(true, null));
    }

    @Test
    public void testNormalizer_Null_To_Integer() {
        assertNull(integerNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_Integer() {
        assertNull(integerNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Null_To_BigDecimal() {
        assertNull(decimalNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_BigDecimal() {
        assertNull(decimalNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Float_To_BigDecimal() {
        assertEquals(BigDecimal.valueOf(1d), decimalNormalizer.normalize((float) 1, null));
    }

    @Test
    public void testNormalizer_Double_To_BigDecimal() {
        assertEquals(BigDecimal.valueOf(1d), decimalNormalizer.normalize((double) 1, null));
    }

    @Test
    public void testNormalizer_BigInteger_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(BigInteger.ONE, null));
    }

    @Test
    public void testNormalizer_BigDecimal_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(BigDecimal.ONE, null));
    }
}
