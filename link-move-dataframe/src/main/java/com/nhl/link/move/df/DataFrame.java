package com.nhl.link.move.df;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Represents immutable 2D data.
 */
public interface DataFrame extends Iterable<DataRow> {

    static DataFrame fromRows(Columns columns, List<DataRow> rows) {
        return new SimpleDataFrame(columns, rows);
    }

    Columns getColumns();

    default DataFrame head(int len) {
        return new HeadDataFrame(this, len);
    }

    default DataFrame map(UnaryOperator<DataRow> f) {
        return map(getColumns(), f);
    }

    default DataFrame map(Columns newColumns, UnaryOperator<DataRow> f) {

        int capacity = estimatedLength();
        List<DataRow> newRows = capacity > 0 ? new ArrayList<>(capacity) : new ArrayList<>();

        for (DataRow r : this) {
            DataRow nr = f.apply(r);
            newRows.add(nr);
        }

        return new SimpleDataFrame(newColumns, newRows);

    }

    default DataFrame renameColumn(String oldName, String newName) {
        return renameColumns(Collections.singletonMap(oldName, newName));
    }

    DataFrame renameColumns(Map<String, String> oldToNewNames);

    <T> DataFrame convertType(String columnName, Class<T> targetType, Function<Object, T> typeConverter);

    @Override
    Iterator<DataRow> iterator();

    int estimatedLength();

    default void consumeAsBatches(Consumer<DataFrame> consumer, int batchSize) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("'batchSize' must be positive: " + batchSize);
        }

        Columns columns = getColumns();
        List<DataRow> rows = new ArrayList<>(batchSize);

        for (DataRow dr : this) {

            rows.add(dr);

            if (rows.size() % batchSize == 0) {
                consumer.accept(DataFrame.fromRows(columns, rows));
                rows = new ArrayList<>(batchSize);
            }
        }

        // consume leftovers
        if (!rows.isEmpty()) {
            consumer.accept(DataFrame.fromRows(columns, rows));
        }
    }
}
