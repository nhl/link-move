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

    private static final LocalDateTimeConverter normalizer = new LocalDateTimeConverter();

    @Test
    public void testConvert_utilDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.convert(date));
    }

    @Test
    public void testConvert_sqlDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.convert(date));
    }

    @Test
    public void testConvert_sqlTime() {
        assertThrows(LmRuntimeException.class, () -> normalizer.convert(new Time(Instant.now().toEpochMilli())));
    }

    @Test
    public void testConvert_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS);
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.convert(timestamp));
    }
}
