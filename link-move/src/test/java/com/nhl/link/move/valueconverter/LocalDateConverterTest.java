package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class LocalDateConverterTest {

    private static final LocalDateConverter CONVERTER = new LocalDateConverter();

    @Test
    public void testConvert_utilDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(date));
    }

    @Test
    public void testConvert_sqlDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(date));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_sqlTime() {
        CONVERTER.convert(new Time(Instant.now().toEpochMilli()));
    }

    @Test
    public void testConvert_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDate, CONVERTER.convert(timestamp));
    }

}
