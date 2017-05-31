package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;

public class LocalTimeNormalizerTest {

    private static final LocalTimeNormalizer normalizer = new LocalTimeNormalizer();

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_utilDate() {
        normalizer.normalize(Date.from(Instant.now()), null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalize_sqlDate() {
        normalizer.normalize(new java.sql.Date(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalize_sqlTime() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        java.sql.Time time = new Time(calendar.getTimeInMillis());
        assertEquals(localTime, normalizer.normalize(time, null));
    }

    @Test
    public void testNormalize_sqlTimestamp() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        assertEquals(localTime, normalizer.normalize(timestamp, null));
    }
}
