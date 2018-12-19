package com.nhl.link.move.df;

import java.util.Objects;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper identity() {
        return r -> r;
    }

    default DataRowMapper andThen(DataRowMapper after) {
        Objects.requireNonNull(after);
        return r -> after.map(map(r));
    }

    /**
     * Maps a row to another row. Returned row's index must be the same as the
     * <code>mapper.mapIndex(row.getIndex())</code>. Note that the default {@link #mapIndex(Index)} would call this
     * method with a row that contains all nulls. So this method should not fail on this condition, or you will need to
     * implement a custom {@link #mapIndex(Index)}.
     *
     * @param row some DataRow.
     * @return a row produced by applying this mapper to the provided row.
     */
    DataRow map(DataRow row);

    default Index mapIndex(Index index) {
        // by default create fake DataRow, run "map", and use the returned DataRow index
        Object[] nulls = new Object[index.size()];
        DataRow template = new ArrayDataRow(index, nulls);
        return map(template).getIndex();
    }
}
