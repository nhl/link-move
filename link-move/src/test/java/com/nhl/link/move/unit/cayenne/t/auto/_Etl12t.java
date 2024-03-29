package com.nhl.link.move.unit.cayenne.t.auto;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.cayenne.BaseDataObject;
import org.apache.cayenne.exp.property.PropertyFactory;
import org.apache.cayenne.exp.property.StringProperty;

/**
 * Class _Etl12t was generated by Cayenne.
 * It is probably a good idea to avoid changing this class manually,
 * since it may be overwritten next time code is regenerated.
 * If you need to make any customizations, please use subclass.
 */
public abstract class _Etl12t extends BaseDataObject {

    private static final long serialVersionUID = 1L;

    public static final String MIXED_CASE_ID_PK_COLUMN = "MixedCaseId";

    public static final StringProperty<String> STARTS_WITH_UPPER_CASE = PropertyFactory.createString("startsWithUpperCase", String.class);

    protected String startsWithUpperCase;


    public void setStartsWithUpperCase(String startsWithUpperCase) {
        beforePropertyWrite("startsWithUpperCase", this.startsWithUpperCase, startsWithUpperCase);
        this.startsWithUpperCase = startsWithUpperCase;
    }

    public String getStartsWithUpperCase() {
        beforePropertyRead("startsWithUpperCase");
        return this.startsWithUpperCase;
    }

    @Override
    public Object readPropertyDirectly(String propName) {
        if(propName == null) {
            throw new IllegalArgumentException();
        }

        switch(propName) {
            case "startsWithUpperCase":
                return this.startsWithUpperCase;
            default:
                return super.readPropertyDirectly(propName);
        }
    }

    @Override
    public void writePropertyDirectly(String propName, Object val) {
        if(propName == null) {
            throw new IllegalArgumentException();
        }

        switch (propName) {
            case "startsWithUpperCase":
                this.startsWithUpperCase = (String)val;
                break;
            default:
                super.writePropertyDirectly(propName, val);
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeSerialized(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        readSerialized(in);
    }

    @Override
    protected void writeState(ObjectOutputStream out) throws IOException {
        super.writeState(out);
        out.writeObject(this.startsWithUpperCase);
    }

    @Override
    protected void readState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.readState(in);
        this.startsWithUpperCase = (String)in.readObject();
    }

}
