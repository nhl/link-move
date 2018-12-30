package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.zip.Zipper;

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

    default DataFrame map(DataRowMapper rowMapper) {
        return map(getColumns(), rowMapper);
    }

    DataFrame map(Index mappedColumns, DataRowMapper rowMapper);

    <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> m);

    default DataFrame renameColumn(String oldName, String newName) {
        return renameColumns(Collections.singletonMap(oldName, newName));
    }

    DataFrame renameColumns(Map<String, String> oldToNewNames);

    /**
     * Returns a DataFrame that combines columns from this and another DataFrame. If the lengths of the DataFrames are
     * not the same, the data from the longest DataFrame is truncated. If two DataFrames have have conflicting columns,
     * an underscore suffix ("_") is added to the column names coming from the right-hand side DataFrame.
     *
     * @param df another DataFrame.
     * @return a new DataFrame that is a combination of columns from this DataFrame and a DataFrame argument.
     */
    default DataFrame zip(DataFrame df) {
        return zip(Zipper.zipIndex(getColumns(), df.getColumns()), df, Zipper::zipRows);
    }

    default DataFrame zip(Index zippedColumns, DataFrame df, DataRowCombiner c) {
        return new ZipDataFrame(zippedColumns, this, df, c);
    }

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
