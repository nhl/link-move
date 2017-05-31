package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class LocalDateTimeNormalizerTest {

    private static final LocalDateTimeNormalizer normalizer = new LocalDateTimeNormalizer();

    @Test
    public void testNormalize_utilDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.normalize(date, null));
    }

    @Test
    public void testNormalize_sqlDate() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.normalize(date, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_sqlTime() {
        normalizer.normalize(new Time(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalize_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDateTime, normalizer.normalize(timestamp, null));
    }
}
