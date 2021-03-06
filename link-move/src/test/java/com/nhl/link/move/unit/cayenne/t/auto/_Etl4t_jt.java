package com.nhl.link.move.unit.cayenne.t.auto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.cayenne.CayenneDataObject;
import org.apache.cayenne.exp.Property;

/**
 * Class _Etl4t_jt was generated by Cayenne.
 * It is probably a good idea to avoid changing this class manually,
 * since it may be overwritten next time code is regenerated.
 * If you need to make any customizations, please use subclass.
 */
public abstract class _Etl4t_jt extends CayenneDataObject {

    private static final long serialVersionUID = 1L; 

    public static final String ID_PK_COLUMN = "id";

    public static final Property<LocalDate> C_DATE = Property.create("cDate", LocalDate.class);
    public static final Property<LocalTime> C_TIME = Property.create("cTime", LocalTime.class);
    public static final Property<LocalDateTime> C_TIMESTAMP = Property.create("cTimestamp", LocalDateTime.class);

    public void setCDate(LocalDate cDate) {
        writeProperty("cDate", cDate);
    }
    public LocalDate getCDate() {
        return (LocalDate)readProperty("cDate");
    }

    public void setCTime(LocalTime cTime) {
        writeProperty("cTime", cTime);
    }
    public LocalTime getCTime() {
        return (LocalTime)readProperty("cTime");
    }

    public void setCTimestamp(LocalDateTime cTimestamp) {
        writeProperty("cTimestamp", cTimestamp);
    }
    public LocalDateTime getCTimestamp() {
        return (LocalDateTime)readProperty("cTimestamp");
    }

}
