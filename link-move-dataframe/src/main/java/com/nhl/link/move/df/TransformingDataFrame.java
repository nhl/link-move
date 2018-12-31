package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;
import java.util.Objects;

/**
 * A DataFrame over an Iterable of unknown (possibly very long) length. Its per-row operations are not applied
 * immediately and are instead deferred until the caller iterates over the contents.
 */
public class TransformingDataFrame implements DataFrame {

    private Iterable<DataRow> source;
    private Index columns;
    private DataRowMapper rowMapper;

    protected TransformingDataFrame(Index columns, Iterable<DataRow> source, DataRowMapper rowMapper) {
        this.source = Objects.requireNonNull(source);
        this.columns = Objects.requireNonNull(columns);
        this.rowMapper = Objects.requireNonNull(rowMapper);
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
                return new ArrayDataRow(columns, rowMapper.map(delegateIt.next()));
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("LazyDataFrame ["), this).append("]").toString();
    }
}
