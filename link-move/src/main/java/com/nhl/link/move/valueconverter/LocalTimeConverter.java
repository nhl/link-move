package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Date;

/**
 * @since 2.2
 */
public class LocalTimeConverter extends SingleTypeConverter<LocalTime> {

    @Override
    protected Class<LocalTime> targetType() {
        return LocalTime.class;
    }

    @Override
    protected LocalTime convertNotNull(Object value, int scale) {

        switch (value.getClass().getName()) {
            case "java.sql.Time":
                java.sql.Time time = (Time) value;
                Instant instant = Instant.ofEpochMilli(time.getTime());
                // java.sql.Time does not set millis when creating a LocalTime
                return time.toLocalTime().plusNanos(instant.getNano());

            case "java.sql.Date":
                throw new LmRuntimeException("Will not perform lossy conversion from LocalTime: " + value);

            case "java.util.Date":
                Date datetime = (Date) value;
                return new Timestamp(datetime.getTime()).toLocalDateTime().toLocalTime();

            case "java.sql.Timestamp":
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime().toLocalTime();

            default:
                throw new LmRuntimeException("Value can not be mapped to LocalTime: " + value);
        }
    }
}
