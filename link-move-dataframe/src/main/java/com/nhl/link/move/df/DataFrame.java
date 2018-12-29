package com.nhl.link.move.df;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Represents immutable 2D data.
 */
public interface DataFrame extends Iterable<DataRow> {

    Index getColumns();

    default DataFrame head(int len) {
        return new HeadDataFrame(this, len);
    }

    DataFrame map(IndexMapper indexMapper, DataRowMapper rowMapper);

    <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> m);

    default DataFrame renameColumn(String oldName, String newName) {
        return renameColumns(Collections.singletonMap(oldName, newName));
    }

    DataFrame renameColumns(Map<String, String> oldToNewNames);

    @Override
    Iterator<DataRow> iterator();

    default void consumeAsBatches(Consumer<DataFrame> consumer, int batchSize) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("'batchSize' must be positive: " + batchSize);
        }

        Index columns = getColumns();
        List<DataRow> rows = new ArrayList<>(batchSize);

        for (DataRow dr : this) {

            rows.add(dr);

            if (rows.size() % batchSize == 0) {
                consumer.accept(new LazyDataFrame(columns, rows));
                rows = new ArrayList<>(batchSize);
            }
        }

        // consume leftovers
        if (!rows.isEmpty()) {
            consumer.accept(new LazyDataFrame(columns, rows));
        }
    }
}
