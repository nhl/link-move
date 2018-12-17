package com.nhl.link.move.df;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A {@link DataFrame} over a fixed collection of rows.
 */
public class SimpleDataFrame implements DataFrame {

    private List<DataRow> rows;
    private Columns columns;

    protected SimpleDataFrame(Columns columns, List<DataRow> rows) {
        this.rows = rows;
        this.columns = columns;
    }

    @Override
    public int estimatedLength() {
        return rows.size();
    }

    @Override
    public Iterator<DataRow> iterator() {
        return rows.iterator();
    }

    @Override
    public Columns getColumns() {
        return columns;
    }

    @Override
    public DataFrame map(Columns newColumns, UnaryOperator<DataRow> f) {
        List<DataRow> newRows = new ArrayList<>(rows.size());

        for (DataRow r : rows) {
            DataRow nr = f.apply(r);
            newRows.add(nr);
        }

        return new SimpleDataFrame(newColumns, newRows);
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Columns newColumns = columns.rename(oldToNewNames);

        List<DataRow> newRows = new ArrayList<>();

        for (DataRow r : rows) {
            DataRow nr = r.columns(newColumns);
            newRows.add(nr);
        }

        return new SimpleDataFrame(newColumns, newRows);
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

        // convert values
        List<DataRow> newRows = new ArrayList<>(rows.size());

        for (DataRow r : rows) {
            newRows.add(r.convertColumn(newColumns, ci, typeConverter));
        }

        return new SimpleDataFrame(newColumns, newRows);
    }
}
