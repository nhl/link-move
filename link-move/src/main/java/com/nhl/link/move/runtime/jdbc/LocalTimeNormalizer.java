package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;

/**
 * @since 2.2
 */
public class LocalTimeNormalizer extends BaseJdbcNormalizer<LocalTime> {

    @Override
    protected LocalTime doNormalize(Object value, DbAttribute targetAttribute) {
        switch (value.getClass().getName()) {
            case "java.sql.Time": {
                java.sql.Time time = (Time) value;
                Instant instant = Instant.ofEpochMilli(time.getTime());
                // java.sql.Time does not set millis when creating a LocalTime
                return time.toLocalTime().plusNanos(instant.getNano());
            }
            case "java.util.Date":
            case "java.sql.Date": {
                throw new LmRuntimeException("Will not perform lossy conversion from LocalTime: " + value);
            }
            case "java.sql.Timestamp": {
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime().toLocalTime();
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to LocalTime: " + value);
            }
        }
    }
}
