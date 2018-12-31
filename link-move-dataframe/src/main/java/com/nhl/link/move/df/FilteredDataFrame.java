package com.nhl.link.move.df;

import com.nhl.link.move.df.filter.DataRowPredicate;
import com.nhl.link.move.df.map.DataRowMapper;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class FilteredDataFrame implements DataFrame {

    private Iterable<DataRow> source;
    private Index columns;
    private DataRowPredicate rowFilter;

    public FilteredDataFrame(Index columns, Iterable<DataRow> source, DataRowPredicate rowFilter) {
        this.source = source;
        this.columns = columns;
        this.rowFilter = rowFilter;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public DataFrame map(Index mappedColumns, DataRowMapper rowMapper) {
        return new LazyDataFrame(mappedColumns, this, rowMapper);
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);
        return new FilteredDataFrame(newColumns, source, rowFilter);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {

            private Iterator<DataRow> delegateIt = FilteredDataFrame.this.source.iterator();
            private DataRow lastResolved;

            {
                rewind();
            }

            private void rewind() {
                lastResolved = null;
                while (delegateIt.hasNext()) {
                    DataRow next = delegateIt.next();
                    if (rowFilter.test(next)) {
                        lastResolved = next;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return lastResolved != null;
            }

            @Override
            public DataRow next() {

                if (lastResolved == null) {
                    throw new NoSuchElementException("No next element");
                }

                DataRow next = lastResolved;
                rewind();
                return next;
            }
        };
    }
}
