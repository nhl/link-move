package com.nhl.link.move.json.unit.cayenne.t.auto;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.cayenne.BaseDataObject;
import org.apache.cayenne.exp.property.NumericProperty;
import org.apache.cayenne.exp.property.PropertyFactory;
import org.apache.cayenne.exp.property.StringProperty;

/**
 * Class _Etlt1 was generated by Cayenne.
 * It is probably a good idea to avoid changing this class manually,
 * since it may be overwritten next time code is regenerated.
 * If you need to make any customizations, please use subclass.
 */
public abstract class _Etlt1 extends BaseDataObject {

    private static final long serialVersionUID = 1L;

    public static final String ID_PK_COLUMN = "id";

    public static final NumericProperty<Integer> NUM_INT = PropertyFactory.createNumeric("numInt", Integer.class);
    public static final StringProperty<String> STRING = PropertyFactory.createString("string", String.class);

    protected Integer numInt;
    protected String string;


    public void setNumInt(Integer numInt) {
        beforePropertyWrite("numInt", this.numInt, numInt);
        this.numInt = numInt;
    }

    public Integer getNumInt() {
        beforePropertyRead("numInt");
        return this.numInt;
    }

    public void setString(String string) {
        beforePropertyWrite("string", this.string, string);
        this.string = string;
    }

    public String getString() {
        beforePropertyRead("string");
        return this.string;
    }

    @Override
    public Object readPropertyDirectly(String propName) {
        if(propName == null) {
            throw new IllegalArgumentException();
        }

        switch(propName) {
            case "numInt":
                return this.numInt;
            case "string":
                return this.string;
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
            case "numInt":
                this.numInt = (Integer)val;
                break;
            case "string":
                this.string = (String)val;
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
        out.writeObject(this.numInt);
        out.writeObject(this.string);
    }

    @Override
    protected void readState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.readState(in);
        this.numInt = (Integer)in.readObject();
        this.string = (String)in.readObject();
    }

}
