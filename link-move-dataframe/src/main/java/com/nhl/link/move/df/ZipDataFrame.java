package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.zip.Zipper;

import java.util.Iterator;
import java.util.Map;

public class ZipDataFrame implements DataFrame {

    private Iterable<DataRow> leftSource;
    private Iterable<DataRow> rightSource;
    private Index columns;
    private DataRowCombiner rowCombiner;

    public ZipDataFrame(
            Index columns,
            Iterable<DataRow> leftSource,
            Iterable<DataRow> rightSource) {
        this(columns, leftSource, rightSource, Zipper::zipRows);
    }

    public ZipDataFrame(
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
    public DataFrame map(Index mappedIndex, DataRowMapper rowMapper) {
        return new LazyDataFrame(mappedIndex, this, rowMapper);
    }

    @Override
    public <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> m) {
        int ci = columns.position(columnName);
        return map(columns, (i, r) -> r.mapColumn(ci, m));
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);
        return new ZipDataFrame(newColumns, leftSource, rightSource, rowCombiner);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {

            private Iterator<DataRow> leftIt = ZipDataFrame.this.leftSource.iterator();
            private Iterator<DataRow> rightIt = ZipDataFrame.this.rightSource.iterator();

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
}
