package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.print.InlinePrinter;
import com.nhl.link.move.df.zip.Zipper;

import java.util.Iterator;
import java.util.Map;

public class ZippingDataFrame implements DataFrame {

    private Iterable<DataRow> leftSource;
    private Iterable<DataRow> rightSource;
    private Index columns;
    private DataRowCombiner rowCombiner;

    public ZippingDataFrame(
            Index columns,
            Iterable<DataRow> leftSource,
            Iterable<DataRow> rightSource) {
        this(columns, leftSource, rightSource, Zipper::zipRows);
    }

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
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);
        return new ZippingDataFrame(newColumns, leftSource, rightSource, rowCombiner);
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
                return rowCombiner.combine(columns, leftIt.next(), rightIt.next());
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("ZipDataFrame ["), this).append("]").toString();
    }
}
