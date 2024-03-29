package com.nhl.link.move.unit.cayenne.t.auto;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;

import org.apache.cayenne.BaseDataObject;
import org.apache.cayenne.exp.property.NumericProperty;
import org.apache.cayenne.exp.property.PropertyFactory;

/**
 * Class _Etl8t was generated by Cayenne.
 * It is probably a good idea to avoid changing this class manually,
 * since it may be overwritten next time code is regenerated.
 * If you need to make any customizations, please use subclass.
 */
public abstract class _Etl8t extends BaseDataObject {

    private static final long serialVersionUID = 1L;

    public static final String ID_PK_COLUMN = "id";

    public static final NumericProperty<BigDecimal> C_DECIMAL1 = PropertyFactory.createNumeric("cDecimal1", BigDecimal.class);
    public static final NumericProperty<BigDecimal> C_DECIMAL2 = PropertyFactory.createNumeric("cDecimal2", BigDecimal.class);
    public static final NumericProperty<BigDecimal> C_DECIMAL3 = PropertyFactory.createNumeric("cDecimal3", BigDecimal.class);

    protected BigDecimal cDecimal1;
    protected BigDecimal cDecimal2;
    protected BigDecimal cDecimal3;


    public void setCDecimal1(BigDecimal cDecimal1) {
        beforePropertyWrite("cDecimal1", this.cDecimal1, cDecimal1);
        this.cDecimal1 = cDecimal1;
    }

    public BigDecimal getCDecimal1() {
        beforePropertyRead("cDecimal1");
        return this.cDecimal1;
    }

    public void setCDecimal2(BigDecimal cDecimal2) {
        beforePropertyWrite("cDecimal2", this.cDecimal2, cDecimal2);
        this.cDecimal2 = cDecimal2;
    }

    public BigDecimal getCDecimal2() {
        beforePropertyRead("cDecimal2");
        return this.cDecimal2;
    }

    public void setCDecimal3(BigDecimal cDecimal3) {
        beforePropertyWrite("cDecimal3", this.cDecimal3, cDecimal3);
        this.cDecimal3 = cDecimal3;
    }

    public BigDecimal getCDecimal3() {
        beforePropertyRead("cDecimal3");
        return this.cDecimal3;
    }

    @Override
    public Object readPropertyDirectly(String propName) {
        if(propName == null) {
            throw new IllegalArgumentException();
        }

        switch(propName) {
            case "cDecimal1":
                return this.cDecimal1;
            case "cDecimal2":
                return this.cDecimal2;
            case "cDecimal3":
                return this.cDecimal3;
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
            case "cDecimal1":
                this.cDecimal1 = (BigDecimal)val;
                break;
            case "cDecimal2":
                this.cDecimal2 = (BigDecimal)val;
                break;
            case "cDecimal3":
                this.cDecimal3 = (BigDecimal)val;
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
        out.writeObject(this.cDecimal1);
        out.writeObject(this.cDecimal2);
        out.writeObject(this.cDecimal3);
    }

    @Override
    protected void readState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.readState(in);
        this.cDecimal1 = (BigDecimal)in.readObject();
        this.cDecimal2 = (BigDecimal)in.readObject();
        this.cDecimal3 = (BigDecimal)in.readObject();
    }

}
