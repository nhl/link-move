package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @since 2.2
 */
public class LocalDateTimeNormalizer extends JdbcNormalizer<LocalDateTime> {

    public LocalDateTimeNormalizer() {
        super(LocalDateTime.class);
    }

    @Override
    protected LocalDateTime doNormalize(Object value, DbAttribute targetAttribute) {
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
                throw new LmRuntimeException("Will not perform lossy conversion from " + getTypeName() + ": " + value);
            }
            case "java.sql.Timestamp": {
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime();
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
