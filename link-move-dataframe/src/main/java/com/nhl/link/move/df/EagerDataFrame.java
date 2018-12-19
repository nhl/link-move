package com.nhl.link.move.df;

import com.nhl.link.move.df.print.InlinePrinter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link DataFrame} over a fixed collection of rows that applies per-rows operations immediately.
 */
public class EagerDataFrame implements DataFrame {

    private List<DataRow> rows;
    private Index columns;

    public EagerDataFrame(Index columns, List<DataRow> rows) {
        this.rows = rows;
        this.columns = columns;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return rows.iterator();
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public DataFrame map(DataRowMapper f) {
        List<DataRow> newRows = new ArrayList<>(rows.size());

        for (DataRow r : rows) {
            DataRow nr = f.apply(r);
            newRows.add(nr);
        }

        return new EagerDataFrame(columns, newRows);
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = columns.rename(oldToNewNames);

        List<DataRow> newRows = new ArrayList<>();

        for (DataRow r : rows) {
            DataRow nr = r.reindex(newColumns);
            newRows.add(nr);
        }

        return new EagerDataFrame(newColumns, newRows);
    }

    @Override
    public <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> typeConverter) {

        int ci = columns.position(columnName);

        List<DataRow> newRows = new ArrayList<>(rows.size());

        for (DataRow r : rows) {
            newRows.add(r.mapColumn(ci, typeConverter));
        }

        return new EagerDataFrame(columns, newRows);
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("EagerDataFrame ["), this).append("]").toString();
    }
}
