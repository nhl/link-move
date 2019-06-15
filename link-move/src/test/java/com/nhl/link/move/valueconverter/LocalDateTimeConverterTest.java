package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class LocalDateTimeConverterTest {

    private static final LocalDateTimeConverter CONVERTER = new LocalDateTimeConverter();

    @Test
    public void testConvert_utilDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(date));
    }

    @Test
    public void testConvert_sqlDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(date));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_sqlTime() {
        CONVERTER.convert(new Time(Instant.now().toEpochMilli()));
    }

    @Test
    public void testConvert_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(timestamp));
    }

    @Test
    public void testConvert_string() {
        LocalDateTime localDateTime = LocalDateTime.of(
                2018, 1, 1, 0, 0, 0
        );
        String date = "2018-01-01T00:00:00";
        assertEquals(localDateTime, CONVERTER.convert(date));
    }
}
