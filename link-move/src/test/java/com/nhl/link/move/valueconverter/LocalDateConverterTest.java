package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LocalDateConverterTest {

    private static final LocalDateConverter CONVERTER = new LocalDateConverter();

    @Test
    public void testConvert_utilDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(date, -1));
    }

    @Test
    public void testConvert_sqlDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(date, -1));
    }

    @Test
    public void testConvert_sqlTime() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(new Time(Instant.now().toEpochMilli()), -1));
    }

    @Test
    public void testConvert_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(timestamp, -1));
    }

    @Test
    public void testConvert_string() {
        assertEquals(LocalDate.of(2017, 1, 2), CONVERTER.convert("2017-01-02", -1));
    }
}
