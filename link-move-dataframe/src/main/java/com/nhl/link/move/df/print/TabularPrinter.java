package com.nhl.link.move.df.print;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;

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

    public StringBuilder print(StringBuilder out, DataFrame df) {
        return newWorker(out).print(df.getColumns(), df.iterator());
    }

    public StringBuilder print(StringBuilder out, DataRow dr) {
        return newWorker(out).print(dr.getIndex(), new Iterator<DataRow>() {

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

    private TabularPrinterWorker newWorker(StringBuilder out) {
        return new TabularPrinterWorker(out, maxDisplayRows, maxDisplayColumnWith);
    }

}
