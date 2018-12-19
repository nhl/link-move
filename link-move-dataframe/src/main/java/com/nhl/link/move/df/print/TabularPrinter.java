package com.nhl.link.move.df.print;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A utility class for outputting DataFrames and DataRows, mainly for debugging purposes.
 */
public class TabularPrinter {

    private static final int MAX_DISPLAY_ROWS = 3;
    private static final int MAX_DISPLAY_COLUMN_WIDTH = 30;

    private static final TabularPrinter DEFAULT_PRINTER = new TabularPrinter();

    private int maxDisplayRows;
    private int maxDisplayColumnWith;

    public TabularPrinter() {
        this(MAX_DISPLAY_ROWS, MAX_DISPLAY_COLUMN_WIDTH);
    }

    public TabularPrinter(int maxDisplayRows, int maxDisplayColumnWith) {
        this.maxDisplayRows = maxDisplayRows;
        this.maxDisplayColumnWith = maxDisplayColumnWith;
    }

    public static TabularPrinter getInstance() {
        return DEFAULT_PRINTER;
    }

    public Sink print(Sink out, DataFrame df) {
        return print(out, df.getColumns(), df.iterator());
    }

    public Sink print(Sink out, DataRow dr) {
        return print(out, dr.getIndex(), new Iterator<DataRow>() {

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

    protected Sink print(Sink out, Index columns, Iterator<DataRow> values) {
        return out;
    }
}
