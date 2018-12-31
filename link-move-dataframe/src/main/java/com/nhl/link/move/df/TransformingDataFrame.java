package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Collections;
import java.util.Iterator;

/**
 * A DataFrame over an Iterable of unknown (possibly very long) length. Its per-row operations are not applied
 * immediately and are instead deferred until the caller iterates over the contents.
 */
public class TransformingDataFrame implements DataFrame {

    private Iterable<DataRow> source;
    private Index columns;
    private DataRowMapper rowMapper;

    public TransformingDataFrame(Index columns) {
        this(columns, Collections.emptyList(), DataRowMapper.reindexMapper());
    }

    public TransformingDataFrame(Index columns, Iterable<DataRow> source) {
        this(columns, source, DataRowMapper.reindexMapper());
    }

    protected TransformingDataFrame(Index columns, Iterable<DataRow> source, DataRowMapper rowMapper) {
        this.source = source;
        this.columns = columns;
        this.rowMapper = rowMapper;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {

            private Iterator<DataRow> delegateIt = TransformingDataFrame.this.source.iterator();

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
