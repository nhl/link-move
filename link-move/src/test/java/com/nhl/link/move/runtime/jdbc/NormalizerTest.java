package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NormalizerTest {

    private static BigIntNormalizer longNormalizer;
    private static BooleanNormalizer booleanNormalizer;
    private static IntegerNormalizer integerNormalizer;
    private static DecimalNormalizer decimalNormalizer;
    private static LocalDateNormalizer localDateNormalizer;
    private static LocalTimeNormalizer localTimeNormalizer;
    private static LocalDateTimeNormalizer localDateTimeNormalizer;

    @BeforeClass
    public static void setUp() {
        longNormalizer = new BigIntNormalizer();
        booleanNormalizer = new BooleanNormalizer();
        decimalNormalizer = new DecimalNormalizer();
        integerNormalizer = new IntegerNormalizer();
        localDateNormalizer = new LocalDateNormalizer();
        localTimeNormalizer = new LocalTimeNormalizer();
        localDateTimeNormalizer = new LocalDateTimeNormalizer();
    }

    @Test
    public void testNormalizer_Null_To_Long() {
        assertNull(longNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_Long() {
        assertNull(longNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Long() {
        assertEquals(Long.valueOf(1), longNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Null_To_Boolean() {
        assertNull(booleanNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_Byte_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Boolean_To_Boolean() {
        assertEquals(true, booleanNormalizer.normalize(true, null));
    }

    @Test
    public void testNormalizer_Null_To_Integer() {
        assertNull(integerNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_Integer() {
        assertNull(integerNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_Integer() {
        assertEquals(Integer.valueOf(1), integerNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Null_To_BigDecimal() {
        assertNull(decimalNormalizer.normalize(null, null));
    }

    @Test
    public void testNormalizer_EmptyString_To_BigDecimal() {
        assertNull(decimalNormalizer.normalize("", null));
    }

    @Test
    public void testNormalizer_String_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize("1", null));
    }

    @Test
    public void testNormalizer_Byte_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((byte) 1, null));
    }

    @Test
    public void testNormalizer_Short_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((short) 1, null));
    }

    @Test
    public void testNormalizer_Integer_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(1, null));
    }

    @Test
    public void testNormalizer_Long_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize((long) 1, null));
    }

    @Test
    public void testNormalizer_Float_To_BigDecimal() {
        assertEquals(BigDecimal.valueOf(1d), decimalNormalizer.normalize((float) 1, null));
    }

    @Test
    public void testNormalizer_Double_To_BigDecimal() {
        assertEquals(BigDecimal.valueOf(1d), decimalNormalizer.normalize((double) 1, null));
    }

    @Test
    public void testNormalizer_BigInteger_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(BigInteger.ONE, null));
    }

    @Test
    public void testNormalizer_BigDecimal_To_BigDecimal() {
        assertEquals(BigDecimal.ONE, decimalNormalizer.normalize(BigDecimal.ONE, null));
    }

    @Test
    public void testNormalizer_utilDate_To_LocalDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDate, localDateNormalizer.normalize(date, null));
    }

    @Test
    public void testNormalizer_sqlDate_To_LocalDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDate, localDateNormalizer.normalize(date, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalizer_sqlTime_To_LocalDate() {
        localDateNormalizer.normalize(new Time(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalizer_sqlTimestamp_To_LocalDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(ZoneId.systemDefault()).toLocalDate();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDate, localDateNormalizer.normalize(timestamp, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalizer_utilDate_To_LocalTime() {
        localTimeNormalizer.normalize(Date.from(Instant.now()), null);
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalizer_sqlDate_To_LocalTime() {
        localTimeNormalizer.normalize(new java.sql.Date(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalizer_sqlTime_To_LocalTime() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        java.sql.Time time = new Time(calendar.getTimeInMillis());
        assertEquals(localTime, localTimeNormalizer.normalize(time, null));
    }

    @Test
    public void testNormalizer_sqlTimestamp_To_LocalTime() {
        LocalTime localTime = LocalTime.now();
        Calendar calendar = new GregorianCalendar(1970, 0, 1);
        calendar.add(Calendar.MILLISECOND, localTime.get(ChronoField.MILLI_OF_DAY));
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        assertEquals(localTime, localTimeNormalizer.normalize(timestamp, null));
    }

    @Test
    public void testNormalizer_utilDate_To_LocalDateTime() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Date date = new Date(now.toEpochMilli());
        assertEquals(localDateTime, localDateTimeNormalizer.normalize(date, null));
    }

    @Test
    public void testNormalizer_sqlDate_To_LocalDateTime() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        java.sql.Date date = new java.sql.Date(now.toEpochMilli());
        assertEquals(localDateTime, localDateTimeNormalizer.normalize(date, null));
    }

    @Test(expected = LmRuntimeException.class)
    public void testNormalizer_sqlTime_To_LocalDateTime() {
        localDateTimeNormalizer.normalize(new Time(Instant.now().toEpochMilli()), null);
    }

    @Test
    public void testNormalizer_sqlTimestamp_To_LocalDateTime() {
        Instant now = Instant.now();
        LocalDateTime localDateTime = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        Timestamp timestamp = new Timestamp(now.toEpochMilli());
        assertEquals(localDateTime, localDateTimeNormalizer.normalize(timestamp, null));
    }
}
