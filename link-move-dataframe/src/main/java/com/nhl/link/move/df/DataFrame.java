package com.nhl.link.move.df;

import com.nhl.link.move.df.filter.DataRowJoinPredicate;
import com.nhl.link.move.df.filter.DataRowPredicate;
import com.nhl.link.move.df.join.IndexedJoiner;
import com.nhl.link.move.df.join.JoinSemantics;
import com.nhl.link.move.df.join.Joiner;
import com.nhl.link.move.df.map.DataRowCombiner;
import com.nhl.link.move.df.map.DataRowMapper;
import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.zip.Zipper;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Represents immutable 2D data.
 */
public interface DataFrame extends Iterable<Object[]> {

    static DataFrame create(Index columns, Iterable<Object[]> source) {
        return new SimpleDataFrame(columns, source);
    }

    static <T> DataFrame create(Index columns, Iterable<T> source, Function<T, Object[]> toArrayMapper) {
        return create(columns, new TransformingIterable<>(source, toArrayMapper));
    }

    Index getColumns();

    default DataFrame head(int len) {
        return new HeadDataFrame(this, len);
    }

    default DataFrame map(DataRowMapper rowMapper) {
        return map(getColumns(), rowMapper);
    }

    default DataFrame map(Index mappedColumns, DataRowMapper rowMapper) {
        return new TransformingDataFrame(mappedColumns, this, rowMapper);
    }

    default <T> DataFrame mapColumn(String columnName, ValueMapper<Object[], T> m) {
        int ci = getColumns().position(columnName).getPosition();
        return mapColumn(ci, m);
    }

    default <T> DataFrame mapColumn(int columnPosition, ValueMapper<Object[], T> m) {
        return map(getColumns(), r -> DataRow.mapColumn(r, columnPosition, m));
    }

    default <T> DataFrame addColumn(String columnName, ValueMapper<Object[], T> m) {
        Index expandedIndex = getColumns().addNames(columnName);
        return map(expandedIndex, r -> DataRow.addColumn(r, m));
    }

    default DataFrame renameColumn(String oldName, String newName) {
        return renameColumns(Collections.singletonMap(oldName, newName));
    }

    default DataFrame renameColumns(Map<String, String> oldToNewNames) {
        Index newColumns = getColumns().rename(oldToNewNames);
        return new TransformingDataFrame(newColumns, this, DataRowMapper.copy());
    }

    default DataFrame filter(DataRowPredicate p) {
        return new FilteredDataFrame(getColumns(), this, p);
    }

    /**
     * Returns a DataFrame that combines columns from this and another DataFrame. If the lengths of the DataFrames are
     * not the same, the data from the longest DataFrame is truncated. If two DataFrames have have conflicting columns,
     * an underscore suffix ("_") is added to the column names coming from the right-hand side DataFrame.
     *
     * @param df another DataFrame.
     * @return a new DataFrame that is a combination of columns from this DataFrame and a DataFrame argument.
     */
    default DataFrame zip(DataFrame df) {
        Index zipIndex = Zipper.zipIndex(getColumns(), df.getColumns());
        return zip(zipIndex, df, Zipper.rowZipper(zipIndex.size()));
    }

    default DataFrame zip(Index zippedColumns, DataFrame df, DataRowCombiner c) {
        return new ZippingDataFrame(zippedColumns, this, df, c);
    }

    default DataFrame join(DataFrame df, DataRowJoinPredicate p) {
        return join(df, new Joiner(p, JoinSemantics.inner));
    }

    default DataFrame join(DataFrame df, Joiner joiner) {
        Index joinedIndex = joiner.joinIndex(getColumns(), df.getColumns());
        return joiner.joinRows(joinedIndex, this, df);
    }

    default DataFrame join(DataFrame df, IndexedJoiner<?> joiner) {
        Index joinedIndex = joiner.joinIndex(getColumns(), df.getColumns());
        return joiner.joinRows(joinedIndex, this, df);
    }

    @Override
    Iterator<Object[]> iterator();
}
