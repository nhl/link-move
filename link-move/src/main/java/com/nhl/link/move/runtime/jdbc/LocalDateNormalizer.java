package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

public class LocalDateNormalizer extends JdbcNormalizer<LocalDate> {

    public LocalDateNormalizer() {
        super(LocalDate.class);
    }

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
            case "java.sql.Time":
            case "java.sql.Timestamp": {
                throw new LmRuntimeException("Will not perform lossy conversion from " + getTypeName() + ": " + value);
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
