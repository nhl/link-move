package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;

public class LocalTimeConverterTest {

    private static final LocalTimeConverter CONVERTER = new LocalTimeConverter();

    @Test
    public void testConvert_utilDate() {
        Date datetime = new Date();
        LocalTime localTime = LocalDateTime.ofInstant(datetime.toInstant(), ZoneId.systemDefault()).toLocalTime();
        assertEquals(localTime, CONVERTER.convert(datetime));
    }

    @Test(expected = LmRuntimeException.class)
    public void testConvert_sqlDate() {
        CONVERTER.convert(new java.sql.Date(Instant.now().toEpochMilli()));
    }

    @Test
    public void testConvert_sqlTime() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        java.sql.Time time = new Time(calendar.getTimeInMillis());
        assertEquals(localTime, CONVERTER.convert(time));
    }

    @Test
    public void testConvert_sqlTimestamp() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        assertEquals(localTime, CONVERTER.convert(timestamp));
    }
}
