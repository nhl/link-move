package com.nhl.link.move.df;

import com.nhl.link.move.df.print.StringBuilderSink;
import com.nhl.link.move.df.print.TabularPrinter;

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

    public LazyDataFrame(Index columns, Iterable<DataRow> source) {
        this(columns, source, DataRowMapper.identity());
    }

    protected LazyDataFrame(Index columns, Iterable<DataRow> source, DataRowMapper rowMapper) {
        this.source = source;
        this.columns = columns;
        this.rowMapper = rowMapper;
    }

    @Override
    public String toString() {
        return TabularPrinter.getInstance().print(new StringBuilderSink(), this).toString();
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);
        DataRowMapper m = r -> r.reindex(newColumns);
        return new LazyDataFrame(newColumns, source, rowMapper.andThen(m));
    }

    @Override
    public DataFrame map(DataRowMapper m) {
        return new LazyDataFrame(columns, source, rowMapper.andThen(m));
    }

    @Override
    public <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> typeConverter) {
        int ci = columns.position(columnName);
        return map(r -> r.mapColumn(ci, typeConverter));
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
                return rowMapper.apply(delegateIt.next());
            }
        };
    }
}
