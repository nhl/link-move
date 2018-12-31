package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;

public class ZippingDataFrame implements DataFrame {

    private Iterable<DataRow> leftSource;
    private Iterable<DataRow> rightSource;
    private Index columns;
    private DataRowCombiner rowCombiner;

    public ZippingDataFrame(
            Index columns,
            Iterable<DataRow> leftSource,
            Iterable<DataRow> rightSource,
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
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {

            private Iterator<DataRow> leftIt = ZippingDataFrame.this.leftSource.iterator();
            private Iterator<DataRow> rightIt = ZippingDataFrame.this.rightSource.iterator();

            @Override
            public boolean hasNext() {
                // implementing "short" iterator
                return leftIt.hasNext() && rightIt.hasNext();
            }

            @Override
            public DataRow next() {
                Object[] values = rowCombiner.combine(leftIt.next(), rightIt.next());
                return new ArrayDataRow(columns, values);
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("ZipDataFrame ["), this).append("]").toString();
    }
}
