package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

/**
 * @since 2.2
 */
public class LocalDateNormalizer extends BaseJdbcNormalizer<LocalDate> {

    @Override
    protected LocalDate doNormalize(Object value, DbAttribute targetAttribute) {
        switch (value.getClass().getName()) {
            case "java.util.Date": {
                Date date = (Date) value;
                return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            }
            case "java.sql.Date": {
                java.sql.Date date = (java.sql.Date) value;
                return date.toLocalDate();
            }
            case "java.sql.Time": {
                throw new LmRuntimeException("Will not perform lossy conversion from LocalDate: " + value);
            }
            case "java.sql.Timestamp": {
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime().toLocalDate();
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to LocalDate: " + value);
            }
        }
    }
}
