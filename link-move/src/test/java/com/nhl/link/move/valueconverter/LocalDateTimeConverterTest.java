package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LocalDateTimeConverterTest {

    private static final LocalDateTimeConverter CONVERTER = new LocalDateTimeConverter();

    @Test
    public void convert_utilDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(date, -1));
    }

    @Test
    public void convert_sqlDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(date, -1));
    }

    @Test
    public void convert_sqlTime() {
        assertThrows(LmRuntimeException.class, () -> CONVERTER.convert(new Time(Instant.now().toEpochMilli()), -1));
    }

    @Test
    public void convert_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDateTime, CONVERTER.convert(timestamp, -1));
    }

    @Test
    public void convert_string() {
        assertEquals(LocalDateTime.of(2017, 1, 2, 1, 0, 1), CONVERTER.convert("2017-01-02T01:00:01", -1));
    }
}
