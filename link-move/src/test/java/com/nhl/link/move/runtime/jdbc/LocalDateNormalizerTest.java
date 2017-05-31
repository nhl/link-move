package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class LocalDateNormalizerTest {

    private static LocalDateNormalizer normalizer = new LocalDateNormalizer();

    @Test
    public void testNormalize_utilDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDate, normalizer.normalize(date, null));
    }

    @Test
    public void testNormalize_sqlDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDate, normalizer.normalize(date, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_sqlTime() {
        normalizer.normalize(new Time(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalize_sqlTimestamp() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDate, normalizer.normalize(timestamp, null));
    }

}
