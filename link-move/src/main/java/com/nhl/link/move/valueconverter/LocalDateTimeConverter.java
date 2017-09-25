package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @since 2.2
 */
public class LocalDateTimeConverter extends SingleTypeConverter<LocalDateTime> {

    @Override
    protected Class<LocalDateTime> targetType() {
        return LocalDateTime.class;
    }

    @Override
    protected LocalDateTime convertNotNull(Object value, int scale) {
        switch (value.getClass().getName()) {
            case "java.util.Date": {
                Date date = (Date) value;
                return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            }
            case "java.sql.Date": {
                java.sql.Date date = (java.sql.Date) value;
                // can't use java.sql.Date#toInstant()
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneId.systemDefault());
            }
            case "java.sql.Time": {
                throw new LmRuntimeException("Will not perform lossy conversion from LocalDateTime: " + value);
            }
            case "java.sql.Timestamp": {
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime();
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to LocalDateTime: " + value);
            }
        }
    }
}
