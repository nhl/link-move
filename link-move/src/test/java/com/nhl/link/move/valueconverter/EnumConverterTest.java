package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnumConverterTest {

    @Test
    public void testConvert_E1() {

        EnumConverter<Enum1> converter = new EnumConverter<>(Enum1.class);

        assertEquals(Enum1.one, converter.convert("one"));
        assertEquals(Enum1.three, converter.convert("three"));
        assertEquals(Enum1.minus_one, converter.convert("minus_one"));
    }

    @Test
    public void testConvert_E2() {

        EnumConverter<Enum2> converter = new EnumConverter<>(Enum2.class);

        assertEquals(Enum2.a, converter.convert("a"));
        assertEquals(Enum2.b, converter.convert("b"));
        assertEquals(Enum2.c, converter.convert("c"));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_E1_InvalidString() {
        EnumConverter<Enum1> converter = new EnumConverter<>(Enum1.class);
        converter.convert("x");
    }

    enum Enum1 {
        one, three, minus_one
    }

    enum Enum2 {
        a, b, c
    }
}
