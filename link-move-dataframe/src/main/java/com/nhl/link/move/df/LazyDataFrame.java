package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.print.InlinePrinter;
import com.nhl.link.move.df.zip.ZipDataRowMapper;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A DataFrame over an Iterable of unknown (possibly very long) length. Its per-row operations are not applied
 * immediately and are instead deferred until the caller iterates over the contents.
 */
public class LazyDataFrame implements DataFrame {

    private Iterable<DataRow> source;
    private Index columns;
    private DataRowMapper rowMapper;

    public LazyDataFrame(Index columns) {
        this(columns, Collections.emptyList(), DataRowMapper.identity());
    }

    public LazyDataFrame(Index columns, Iterable<DataRow> source) {
        this(columns, source, DataRowMapper.identity());
    }

    protected LazyDataFrame(Index columns, Iterable<DataRow> source, DataRowMapper rowMapper) {
        this.source = source;
        this.columns = columns;
        this.rowMapper = rowMapper;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);
        return new LazyDataFrame(newColumns, this, DataRowMapper.identity());
    }

    @Override
    public DataFrame map(Index mappedIndex, DataRowMapper rowMapper) {
        return new LazyDataFrame(mappedIndex, this, rowMapper);
    }

    @Override
    public DataFrame zip(DataFrame df) {
        Index zippedColumns = ZipDataRowMapper.zipIndex(getColumns(), df.getColumns());
        DataRowMapper rowMapper = new ZipDataRowMapper(this.iterator());
        return df.map(zippedColumns, rowMapper);
    }

    @Override
    public <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> typeConverter) {
        int ci = columns.position(columnName);
        return map(columns, (i, r) -> r.mapColumn(ci, typeConverter));
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {

            private Iterator<DataRow> delegateIt = LazyDataFrame.this.source.iterator();

            @Override
            public boolean hasNext() {
                return delegateIt.hasNext();
            }

            @Override
            public DataRow next() {
                return rowMapper.map(columns, delegateIt.next());
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("LazyDataFrame ["), this).append("]").toString();
    }
}
