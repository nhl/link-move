package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EnumConverterTest {

    @Test
    public void testConvert_E1() {

        EnumConverter<Enum1> converter = new EnumConverter<>(Enum1.class);

        assertEquals(Enum1.one, converter.convert("one", -1));
        assertEquals(Enum1.three, converter.convert("three", -1));
        assertEquals(Enum1.minus_one, converter.convert("minus_one", -1));
    }

    @Test
    public void testConvert_E2() {

        EnumConverter<Enum2> converter = new EnumConverter<>(Enum2.class);

        assertEquals(Enum2.a, converter.convert("a", -1));
        assertEquals(Enum2.b, converter.convert("b", -1));
        assertEquals(Enum2.c, converter.convert("c", -1));
    }

    @Test
    public void testConvert_E1_InvalidString() {
        EnumConverter<Enum1> converter = new EnumConverter<>(Enum1.class);
        assertThrows(LmRuntimeException.class, () -> converter.convert("x", -1));
    }

    enum Enum1 {
        one, three, minus_one
    }

    enum Enum2 {
        a, b, c
    }
}
