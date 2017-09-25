package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

/**
 * @since 2.2
 */
public class LocalDateConverter extends SingleTypeConverter<LocalDate> {

    @Override
    protected Class<LocalDate> targetType() {
        return LocalDate.class;
    }

    @Override
    protected LocalDate convertNotNull(Object value, int scale) {

        switch (value.getClass().getName()) {
            case "java.util.Date":
                Date utilDate = (Date) value;
                return utilDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

            case "java.sql.Date":
                java.sql.Date sqlDate = (java.sql.Date) value;
                return sqlDate.toLocalDate();

            case "java.sql.Time":
                throw new LmRuntimeException("Will not perform lossy conversion from LocalDate: " + value);

            case "java.sql.Timestamp":
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime().toLocalDate();

            default:
                throw new LmRuntimeException("Value can not be mapped to LocalDate: " + value);
        }
    }
}
