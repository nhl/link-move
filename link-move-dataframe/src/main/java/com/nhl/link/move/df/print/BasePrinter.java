package com.nhl.link.move.df.print;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class BasePrinter {

    private static final int MAX_DISPLAY_ROWS = 3;
    private static final int MAX_DISPLAY_COLUMN_WIDTH = 30;

    protected int maxDisplayRows;
    protected int maxDisplayColumnWith;

    protected BasePrinter() {
        this(MAX_DISPLAY_ROWS, MAX_DISPLAY_COLUMN_WIDTH);
    }

    protected BasePrinter(int maxDisplayRows, int maxDisplayColumnWith) {
        this.maxDisplayRows = maxDisplayRows;
        this.maxDisplayColumnWith = maxDisplayColumnWith;
    }

    public String toString(DataFrame df) {
        return print(new StringBuilder(), df).toString();
    }

    public String toString(DataRow dr) {
        return print(new StringBuilder(), dr).toString();
    }

    public StringBuilder print(StringBuilder out, DataFrame df) {
        return newWorker(out).print(df.getColumns(), df.iterator());
    }

    public StringBuilder print(StringBuilder out, DataRow dr) {
        return newWorker(out).print(dr.getColumns(), new Iterator<DataRow>() {

            int count;

            @Override
            public boolean hasNext() {
                return count == 0;
            }

            @Override
            public DataRow next() {

                if (!hasNext()) {
                    throw new NoSuchElementException(String.valueOf(count));
                }

                count++;
                return dr;
            }
        });
    }

    protected abstract BasePrinterWorker newWorker(StringBuilder out);
}
