package com.nhl.link.move.df;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A DataFrame over an iterable of unknown (possibly very long) length. Most operations on this DataFrame are not applied
 * immediately and are instead deferred until the caller iterates over the contents.
 */
public class LazyDataFrame implements DataFrame {

    private Iterable<DataRow> source;
    private Columns columns;
    private UnaryOperator<DataRow> rowTransformer;

    public LazyDataFrame(Columns columns, Iterable<DataRow> source) {
        this(columns, source, UnaryOperator.identity());
    }

    protected LazyDataFrame(Columns columns, Iterable<DataRow> source, UnaryOperator<DataRow> rowTransformer) {
        this.source = source;
        this.columns = columns;
        this.rowTransformer = rowTransformer;
    }

    @Override
    public Columns getColumns() {
        return columns;
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Columns newColumns = columns.rename(oldToNewNames);
        UnaryOperator<DataRow> newTransformer = r -> rowTransformer.apply(r).columns(newColumns);
        return new LazyDataFrame(newColumns, source, newTransformer);
    }

    @Override
    public <T> DataFrame convertType(String columnName, Class<T> targetType, Function<Object, T> typeConverter) {

        int ci = columns.position(columnName);

        int width = columns.size();

        // convert header
        Column<?>[] newColumnsArray = new Column<?>[width];
        System.arraycopy(columns.getColumns(), 0, newColumnsArray, 0, width);
        newColumnsArray[ci] = new Column<>(columnName, targetType);
        Columns newColumns = new Columns(newColumnsArray);

        UnaryOperator<DataRow> newTransformer = r -> rowTransformer.apply(r).convertColumn(newColumns, ci, typeConverter);

        return new LazyDataFrame(newColumns, source, newTransformer);
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
                return rowTransformer.apply(delegateIt.next());
            }
        };
    }

    @Override
    public int estimatedLength() {
        return -1;
    }
}
