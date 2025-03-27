package com.nhl.link.move.valueconverter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BooleanConverterTest {

    private static final BooleanConverter CONVERTER = new BooleanConverter();

    @Test
    public void testConvert_Null() {
        assertNull(CONVERTER.convert(null, -1));
    }

    @Test
    public void testConvert_Byte() {
        assertEquals(true, CONVERTER.convert((byte) 1, -1));
    }

    @Test
    public void testConvert_Short() {
        assertEquals(true, CONVERTER.convert((short) 1, -1));
        assertEquals(false, CONVERTER.convert((short) 0, -1));
    }

    @Test
    public void testConvert_Integer() {
        assertEquals(true, CONVERTER.convert(1, -1));
    }

    @Test
    public void testConvert_Long() {
        assertEquals(true, CONVERTER.convert((long) 1, -1));
        assertEquals(false, CONVERTER.convert((long) 0, -1));
    }

    @Test
    public void testConvert_Boolean() {
        assertEquals(true, CONVERTER.convert(true, -1));
    }
}
