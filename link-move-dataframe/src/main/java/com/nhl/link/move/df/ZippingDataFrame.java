package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;

public class ZippingDataFrame implements DataFrame {

    private Iterable<Object[]> leftSource;
    private Iterable<Object[]> rightSource;
    private Index columns;
    private DataRowCombiner rowCombiner;

    public ZippingDataFrame(
            Index columns,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            DataRowCombiner rowCombiner) {

        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.columns = columns;
        this.rowCombiner = rowCombiner;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {

            private int width = ZippingDataFrame.this.columns.size();
            private Iterator<Object[]> leftIt = ZippingDataFrame.this.leftSource.iterator();
            private Iterator<Object[]> rightIt = ZippingDataFrame.this.rightSource.iterator();

            @Override
            public boolean hasNext() {
                // implementing "short" iterator
                return leftIt.hasNext() && rightIt.hasNext();
            }

            @Override
            public Object[] next() {
                Object[] zipped = rowCombiner.combine(leftIt.next(), rightIt.next());

                if (width != zipped.length) {
                    throw new IllegalStateException(String.format(
                            "Index size of %s is not the same as values size of %s",
                            width,
                            zipped.length));
                }

                return zipped;
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("ZipDataFrame ["), this).append("]").toString();
    }
}
